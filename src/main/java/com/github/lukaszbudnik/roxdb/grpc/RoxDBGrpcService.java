package com.github.lukaszbudnik.roxdb.grpc;

import com.github.lukaszbudnik.roxdb.rocksdb.Item;
import com.github.lukaszbudnik.roxdb.rocksdb.Key;
import com.github.lukaszbudnik.roxdb.rocksdb.RoxDB;
import com.github.lukaszbudnik.roxdb.v1.ItemRequest;
import com.github.lukaszbudnik.roxdb.v1.ItemResponse;
import com.github.lukaszbudnik.roxdb.v1.RoxDBGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

        ItemResponse.Builder responseBuilder =
            ItemResponse.newBuilder().setCorrelationId(itemRequest.getCorrelationId());
        try {

          switch (itemRequest.getOperationCase()) {
            case PUT_ITEM -> putItem(itemRequest.getPutItem(), responseBuilder);

            case UPDATE_ITEM -> updateItem(itemRequest.getUpdateItem(), responseBuilder);

            case GET_ITEM -> getItem(itemRequest.getGetItem(), responseBuilder);

            case DELETE_ITEM -> deleteItem(itemRequest.getDeleteItem(), responseBuilder);

            case QUERY -> query(itemRequest.getQuery(), responseBuilder);

            case TRANSACT_WRITE_ITEMS ->
                transactWriteItems(itemRequest.getTransactWriteItems(), responseBuilder);
          }

        } catch (RocksDBException e) {
          responseBuilder.setError(
              ItemResponse.Error.newBuilder()
                  .setMessage(Error.ROCKS_DB_ERROR.getMessage())
                  .setCode(Error.ROCKS_DB_ERROR.getCode())
                  .build());
        } catch (Throwable t) {
          onError(t);
        } finally {
          responseObserver.onNext(responseBuilder.build());
          onCompleted();
        }
      }

      @Override
      public void onError(Throwable t) {
        logger.error("Error processing item request", t);
        responseObserver.onError(Status.INTERNAL.withDescription("Internal error").asException());
      }

      @Override
      public void onCompleted() {
        responseObserver.onCompleted();
      }
    };
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
          ItemResponse.GetItemResponse.newBuilder()
              .setItem(ProtoUtils.itemModelToProto(item))
              .build());
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
    var items =
        roxDB.query(
            tableName,
            query.getPartitionKey(),
            Optional.of(query.getSortKeyStart()),
            Optional.of(query.getSortKeyEnd()));
    var itemsQueryResultBuilder = ItemResponse.QueryResponse.ItemsQueryResult.newBuilder();
    for (var item : items) {
      var protoItem = ProtoUtils.itemModelToProto(item);
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
                    ProtoUtils.itemProtoToModel(transactWriteItem.getPut().getItem()));
                modifiedKeys.add(transactWriteItem.getPut().getItem().getKey());
              }
              case UPDATE -> {
                txCtx.update(
                    transactWriteItem.getUpdate().getTable(),
                    ProtoUtils.itemProtoToModel(transactWriteItem.getUpdate().getItem()));
                modifiedKeys.add(transactWriteItem.getUpdate().getItem().getKey());
              }
              case DELETE -> {
                txCtx.delete(
                    transactWriteItem.getDelete().getTable(),
                    ProtoUtils.keyProtoToModel(transactWriteItem.getDelete().getKey()));
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
