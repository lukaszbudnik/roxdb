package com.github.lukaszbudnik.roxdb.grpc;

import com.github.lukaszbudnik.roxdb.rocksdb.*;
import com.github.lukaszbudnik.roxdb.v1.ItemRequest;
import com.github.lukaszbudnik.roxdb.v1.ItemResponse;
import com.github.lukaszbudnik.roxdb.v1.RoxDBGrpc;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.*;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoxDBGrpcService extends RoxDBGrpc.RoxDBImplBase {
  private static final Logger logger = LoggerFactory.getLogger(RoxDBGrpcService.class);
  private final RoxDB roxDB;

  public RoxDBGrpcService(RoxDB roxDB) {
    this.roxDB = roxDB;
  }

  @Override
  public StreamObserver<ItemRequest> processItems(StreamObserver<ItemResponse> responseObserver) {
    return new StreamObserver<ItemRequest>() {
      @Override
      public void onNext(ItemRequest itemRequest) {

        try {
          ItemResponse.Builder responseBuilder =
              ItemResponse.newBuilder().setCorrelationId(itemRequest.getCorrelationId());

          List<ValidationResult> validationResults = validateKeys(itemRequest);
          Optional<ValidationResult> invalidResult =
              validationResults.stream().filter(r -> !r.valid()).findFirst();
          if (invalidResult.isPresent()) {
            List<ItemResponse.Error> errors =
                validationResults.stream()
                    .filter(vr -> !vr.valid())
                    .map(
                        vr -> ItemResponse.Error.newBuilder().setMessage(vr.errorMessage()).build())
                    .toList();
            responseBuilder.setErrors(ItemResponse.Errors.newBuilder().addAllError(errors).build());
          } else {
            executeOperation(itemRequest, responseBuilder);
          }
          responseObserver.onNext(responseBuilder.build());
        } catch (Throwable t) {
          Status status = Status.INTERNAL.withDescription("Internal server error").withCause(t);
          Metadata metadata = new Metadata();
          metadata.put(
              Metadata.Key.of("correlationId", Metadata.ASCII_STRING_MARSHALLER),
              itemRequest.getCorrelationId());
          metadata.put(
              Metadata.Key.of("exception", Metadata.ASCII_STRING_MARSHALLER),
              t.getClass().getName());
          onError(status, metadata);
        }
      }

      @Override
      public void onError(Throwable t) {
        onError(Status.INTERNAL.withDescription("Internal server error"), null);
      }

      private void onError(Status status, Metadata metadata) {
        logger.error("Error processing item request", status.getCause());
        responseObserver.onError(status.asException(metadata));
      }

      @Override
      public void onCompleted() {
        responseObserver.onCompleted();
      }
    };
  }

  private List<ValidationResult> validateKeys(ItemRequest itemRequest) {
    return switch (itemRequest.getOperationCase()) {
      case PUT_ITEM -> validateSingleKey(itemRequest.getPutItem().getItem().getKey());
      case UPDATE_ITEM -> validateSingleKey(itemRequest.getUpdateItem().getItem().getKey());
      case GET_ITEM -> validateSingleKey(itemRequest.getGetItem().getKey());
      case DELETE_ITEM -> validateSingleKey(itemRequest.getDeleteItem().getKey());
      case QUERY -> validateQueryKeys(itemRequest.getQuery());
      case TRANSACT_WRITE_ITEMS -> validateTransactionKeys(itemRequest.getTransactWriteItems());
      default -> throw new IllegalArgumentException("Operation not set");
    };
  }

  private List<ValidationResult> validateSingleKey(com.github.lukaszbudnik.roxdb.v1.Key key) {
    return KeyValidator.isValid(ProtoUtils.protoToModel(key));
  }

  private List<ValidationResult> validateQueryKeys(ItemRequest.Query query) {
    // When set SortKeyRange must have at least one boundary
    if (query.hasSortKeyRange()
        && !query.getSortKeyRange().hasStart()
        && !query.getSortKeyRange().hasEnd()) {
      return List.of(
          new ValidationResult(false, "When set SortKeyRange must have at least one boundary"));
    }

    String sortKeyStart = null;
    if (query.hasSortKeyRange() && query.getSortKeyRange().hasStart()) {
      sortKeyStart = query.getSortKeyRange().getStart().getValue();
    }
    String sortKeyEnd = null;
    if (query.hasSortKeyRange() && query.getSortKeyRange().hasEnd()) {
      sortKeyEnd = query.getSortKeyRange().getEnd().getValue();
    }

    // query allows empty sort keys so make them non-empty as the default validation will raise
    // errors
    if (sortKeyStart == null) {
      sortKeyStart = " ";
    }
    if (sortKeyEnd == null) {
      sortKeyEnd = " ";
    }

    Key startKey = new Key(query.getPartitionKey(), sortKeyStart);
    Key endKey = new Key(query.getPartitionKey(), sortKeyEnd);

    List<ValidationResult> startKeyValidationResult = KeyValidator.isValid(startKey);
    List<ValidationResult> endKeyValidationResult = KeyValidator.isValid(endKey);

    // create a list of validation results from startKeyValidationResult and endKeyValidationResult
    List<ValidationResult> validationResults = new ArrayList<>(startKeyValidationResult);
    // elements of the endKeyValidationResult are added to the final list
    // only if they are not already present (since the primary key is the same for both we don't
    // want duplicate errors for primary key)
    endKeyValidationResult.stream()
        .filter(vr -> !validationResults.contains(vr))
        .forEach(validationResults::add);

    return validationResults;
  }

  private List<ValidationResult> validateTransactionKeys(
      ItemRequest.TransactWriteItems transactWriteItems) {
    return transactWriteItems.getItemsList().stream()
        .map(
            item ->
                switch (item.getOperationCase()) {
                  case PUT -> item.getPut().getItem().getKey();
                  case UPDATE -> item.getUpdate().getItem().getKey();
                  case DELETE -> item.getDelete().getKey();
                  default -> throw new IllegalArgumentException("Invalid operation");
                })
        .map(ProtoUtils::protoToModel)
        .map(KeyValidator::isValid)
        .flatMap(Collection::stream)
        .toList();
  }

  private void executeOperation(ItemRequest itemRequest, ItemResponse.Builder responseBuilder)
      throws RocksDBException {
    switch (itemRequest.getOperationCase()) {
      case PUT_ITEM -> putItem(itemRequest.getPutItem(), responseBuilder);
      case UPDATE_ITEM -> updateItem(itemRequest.getUpdateItem(), responseBuilder);
      case GET_ITEM -> getItem(itemRequest.getGetItem(), responseBuilder);
      case DELETE_ITEM -> deleteItem(itemRequest.getDeleteItem(), responseBuilder);
      case QUERY -> query(itemRequest.getQuery(), responseBuilder);
      case TRANSACT_WRITE_ITEMS ->
          transactWriteItems(itemRequest.getTransactWriteItems(), responseBuilder);
    }
  }

  private void putItem(ItemRequest.PutItem putItem, ItemResponse.Builder responseBuilder)
      throws RocksDBException {
    var protoItem = putItem.getItem();
    var tableName = putItem.getTable();
    Map<String, Object> attributes = ProtoUtils.structToMap(protoItem.getAttributes());
    var key = new Key(protoItem.getKey().getPartitionKey(), protoItem.getKey().getSortKey());
    var item = new Item(key, attributes);
    roxDB.putItem(tableName, item);

    responseBuilder.setPutItemResponse(
        ItemResponse.PutItemResponse.newBuilder().setKey(putItem.getItem().getKey()).build());
  }

  private void updateItem(ItemRequest.UpdateItem updateItem, ItemResponse.Builder responseBuilder)
      throws RocksDBException {
    var protoItem = updateItem.getItem();
    var tableName = updateItem.getTable();
    Map<String, Object> attributes = ProtoUtils.structToMap(protoItem.getAttributes());
    var key = new Key(protoItem.getKey().getPartitionKey(), protoItem.getKey().getSortKey());
    var item = new Item(key, attributes);
    roxDB.updateItem(tableName, item);

    responseBuilder.setUpdateItemResponse(
        ItemResponse.UpdateItemResponse.newBuilder().setKey(updateItem.getItem().getKey()).build());
  }

  private void getItem(ItemRequest.GetItem getItem, ItemResponse.Builder responseBuilder)
      throws RocksDBException {
    String tableName = getItem.getTable();
    var key = new Key(getItem.getKey().getPartitionKey(), getItem.getKey().getSortKey());
    var item = roxDB.getItem(tableName, key);
    if (item != null) {
      responseBuilder.setGetItemResponse(
          ItemResponse.GetItemResponse.newBuilder().setItem(ProtoUtils.modelToProto(item)).build());
    } else {
      responseBuilder.setGetItemResponse(
          ItemResponse.GetItemResponse.newBuilder()
              .setItemNotFound(
                  ItemResponse.GetItemResponse.ItemNotFound.newBuilder()
                      .setKey(getItem.getKey())
                      .build())
              .build());
    }
  }

  private void deleteItem(ItemRequest.DeleteItem deleteItem, ItemResponse.Builder responseBuilder)
      throws RocksDBException {
    String tableName = deleteItem.getTable();
    var key = new Key(deleteItem.getKey().getPartitionKey(), deleteItem.getKey().getSortKey());
    roxDB.deleteItem(tableName, key);

    responseBuilder.setDeleteItemResponse(
        ItemResponse.DeleteItemResponse.newBuilder().setKey(deleteItem.getKey()).build());
  }

  private void query(ItemRequest.Query query, ItemResponse.Builder responseBuilder)
      throws RocksDBException {
    String tableName = query.getTable();
    Optional<SortKeyRange> sortKeyRange = Optional.empty();
    if (query.hasSortKeyRange()) {
      SortKeyRange modelSortKeyRange = ProtoUtils.protoToModel(query.getSortKeyRange());
      sortKeyRange = Optional.of(modelSortKeyRange);
    }
    int limit = query.getLimit();
    var items = roxDB.query(tableName, query.getPartitionKey(), limit, sortKeyRange);
    var itemsQueryResultBuilder = ItemResponse.QueryResponse.ItemsQueryResult.newBuilder();
    for (var item : items) {
      var protoItem = ProtoUtils.modelToProto(item);
      itemsQueryResultBuilder.addItems(protoItem);
    }

    responseBuilder.setQueryResponse(
        ItemResponse.QueryResponse.newBuilder()
            .setItemsQueryResult(itemsQueryResultBuilder.build())
            .build());
  }

  private void transactWriteItems(
      ItemRequest.TransactWriteItems transactWriteItems, ItemResponse.Builder responseBuilder)
      throws RocksDBException {
    List<com.github.lukaszbudnik.roxdb.v1.Key> modifiedKeys = new ArrayList<>();

    roxDB.executeTransaction(
        (txCtx) -> {
          for (ItemRequest.TransactWriteItem transactWriteItem :
              transactWriteItems.getItemsList()) {
            switch (transactWriteItem.getOperationCase()) {
              case PUT -> {
                txCtx.put(
                    transactWriteItem.getPut().getTable(),
                    ProtoUtils.protoToModel(transactWriteItem.getPut().getItem()));
                modifiedKeys.add(transactWriteItem.getPut().getItem().getKey());
              }
              case UPDATE -> {
                txCtx.update(
                    transactWriteItem.getUpdate().getTable(),
                    ProtoUtils.protoToModel(transactWriteItem.getUpdate().getItem()));
                modifiedKeys.add(transactWriteItem.getUpdate().getItem().getKey());
              }
              case DELETE -> {
                txCtx.delete(
                    transactWriteItem.getDelete().getTable(),
                    ProtoUtils.protoToModel(transactWriteItem.getDelete().getKey()));
                modifiedKeys.add(transactWriteItem.getDelete().getKey());
              }
            }
          }
        });

    responseBuilder.setTransactWriteItemsResponse(
        ItemResponse.TransactWriteItemsResponse.newBuilder()
            .setTransactWriteItemsResult(
                ItemResponse.TransactWriteItemsResponse.TransactWriteItemsResult.newBuilder()
                    .addAllModifiedKeys(modifiedKeys)
                    .build())
            .build());
  }
}
