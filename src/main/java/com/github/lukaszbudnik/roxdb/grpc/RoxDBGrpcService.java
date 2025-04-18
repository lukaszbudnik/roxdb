package com.github.lukaszbudnik.roxdb.grpc;

import com.github.lukaszbudnik.roxdb.rocksdb.Item;
import com.github.lukaszbudnik.roxdb.rocksdb.Key;
import com.github.lukaszbudnik.roxdb.rocksdb.RoxDB;
import com.github.lukaszbudnik.roxdb.v1.ItemRequest;
import com.github.lukaszbudnik.roxdb.v1.ItemResponse;
import com.github.lukaszbudnik.roxdb.v1.RoxDBGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
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

        try {

          if (itemRequest.hasPutItem()) {
            putItem(responseObserver, itemRequest.getPutItem());
            responseObserver.onNext(
                ItemResponse.newBuilder()
                    .setCorrelationId(itemRequest.getCorrelationId())
                    .setPutItemResponse(
                        ItemResponse.PutItemResponse.newBuilder()
                            .setKey(itemRequest.getPutItem().getItem().getKey())
                            .build())
                    .build());
          }
          if (itemRequest.hasUpdateItem()) {
            updateItem(responseObserver, itemRequest.getUpdateItem());
            responseObserver.onNext(
                ItemResponse.newBuilder()
                    .setCorrelationId(itemRequest.getCorrelationId())
                    .setUpdateItemResponse(
                        ItemResponse.UpdateItemResponse.newBuilder()
                            .setKey(itemRequest.getUpdateItem().getItem().getKey())
                            .build())
                    .build());
          }
          if (itemRequest.hasGetItem()) {
            var item = getItem(responseObserver, itemRequest.getGetItem());
            if (item != null) {
              responseObserver.onNext(
                  ItemResponse.newBuilder()
                      .setCorrelationId(itemRequest.getCorrelationId())
                      .setGetItemResponse(
                          ItemResponse.GetItemResponse.newBuilder().setItem(item).build())
                      .build());
            } else {
              responseObserver.onNext(
                  ItemResponse.newBuilder()
                      .setCorrelationId(itemRequest.getCorrelationId())
                      .setGetItemResponse(
                          ItemResponse.GetItemResponse.newBuilder()
                              .setItemNotFound(
                                  ItemResponse.GetItemResponse.ItemNotFound.newBuilder()
                                      .setKey(itemRequest.getGetItem().getKey())
                                      .build())
                              .build())
                      .build());
            }
          }
          if (itemRequest.hasDeleteItem()) {
            deleteItem(responseObserver, itemRequest.getDeleteItem());
            responseObserver.onNext(
                ItemResponse.newBuilder()
                    .setCorrelationId(itemRequest.getCorrelationId())
                    .setDeleteItemResponse(
                        ItemResponse.DeleteItemResponse.newBuilder()
                            .setKey(itemRequest.getDeleteItem().getKey())
                            .build())
                    .build());
          }
          if (itemRequest.hasQuery()) {
            var items = query(responseObserver, itemRequest.getQuery());
            responseObserver.onNext(
                ItemResponse.newBuilder()
                    .setCorrelationId(itemRequest.getCorrelationId())
                    .setQueryResponse(
                        ItemResponse.QueryResponse.newBuilder()
                            .setItemsQueryResult(items.build())
                            .build())
                    .build());
          }

        } catch (RocksDBException e) {
          responseObserver.onNext(
              ItemResponse.newBuilder()
                  .setCorrelationId(itemRequest.getCorrelationId())
                  .setError(
                      ItemResponse.Error.newBuilder()
                          .setMessage(Error.ROCKS_DB_ERROR.getMessage())
                          .setCode(Error.ROCKS_DB_ERROR.getCode())
                          .build())
                  .build());
        } catch (Throwable t) {
          onError(t);
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

  private void putItem(StreamObserver<ItemResponse> responseObserver, ItemRequest.PutItem putItem)
      throws RocksDBException {
    var protoItem = putItem.getItem();
    var tableName = putItem.getTable();
    Map<String, Object> attributes = ProtoUtils.structToMap(protoItem.getAttributes());
    var key = new Key(protoItem.getKey().getPartitionKey(), protoItem.getKey().getSortKey());
    var item = new Item(key, attributes);
    roxDB.putItem(tableName, item);
  }

  private void updateItem(
      StreamObserver<ItemResponse> responseObserver, ItemRequest.UpdateItem updateItem)
      throws RocksDBException {
    var protoItem = updateItem.getItem();
    var tableName = updateItem.getTable();
    Map<String, Object> attributes = ProtoUtils.structToMap(protoItem.getAttributes());
    var key = new Key(protoItem.getKey().getPartitionKey(), protoItem.getKey().getSortKey());
    var item = new Item(key, attributes);
    roxDB.updateItem(tableName, item);
  }

  private com.github.lukaszbudnik.roxdb.v1.Item getItem(
      StreamObserver<ItemResponse> responseObserver, ItemRequest.GetItem getItem)
      throws RocksDBException {
    String tableName = getItem.getTable();
    var key = new Key(getItem.getKey().getPartitionKey(), getItem.getKey().getSortKey());
    var item = roxDB.getItem(tableName, key);
    if (item == null) {
      return null;
    }
    return ProtoUtils.itemToProto(item);
  }

  private void deleteItem(
      StreamObserver<ItemResponse> responseObserver, ItemRequest.DeleteItem deleteItem)
      throws RocksDBException {
    String tableName = deleteItem.getTable();
    var key = new Key(deleteItem.getKey().getPartitionKey(), deleteItem.getKey().getSortKey());
    roxDB.deleteItem(tableName, key);
  }

  private ItemResponse.QueryResponse.ItemsQueryResult.Builder query(
      StreamObserver<ItemResponse> responseObserver, ItemRequest.Query query)
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
      var protoItem = ProtoUtils.itemToProto(item);
      itemsQueryResultBuilder.addItems(protoItem);
    }
    return itemsQueryResultBuilder;
  }
}
