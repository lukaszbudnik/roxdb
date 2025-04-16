package com.github.lukaszbudnik.roxdb.grpc;

import com.github.lukaszbudnik.roxdb.api.Item;
import com.github.lukaszbudnik.roxdb.api.Key;
import com.github.lukaszbudnik.roxdb.api.RoxDB;
import com.github.lukaszbudnik.roxdb.v1.ItemRequest;
import com.github.lukaszbudnik.roxdb.v1.ItemResponse;
import com.github.lukaszbudnik.roxdb.v1.RoxDBGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.rocksdb.RocksDBException;

import java.util.Map;
import java.util.Optional;

public class RoxDBGrpcService extends RoxDBGrpc.RoxDBImplBase {
    public static final int ROCKS_DB_ERROR = 1;
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
                        putItem(responseObserver, itemRequest);
                    }
                    if (itemRequest.hasGetItem()) {
                        getItem(responseObserver, itemRequest);
                    }
                    if (itemRequest.hasDeleteItem()) {
                        deleteItem(responseObserver, itemRequest);
                    }
                    if (itemRequest.hasQuery()) {
                        query(responseObserver, itemRequest);
                    }

                } catch (RocksDBException e) {
                    responseObserver.onNext(ItemResponse.newBuilder().setCorrelationId(itemRequest.getCorrelationId()).setError(ItemResponse.Error.newBuilder().setMessage(e.getMessage()).setCode(ROCKS_DB_ERROR).build()).build());
                } catch (Exception e) {
                    responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
                }

            }

            @Override
            public void onError(Throwable throwable) {
                responseObserver.onError(throwable);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }

    private void putItem(StreamObserver<ItemResponse> responseObserver, ItemRequest itemRequest) throws RocksDBException {
        var putItem = itemRequest.getPutItem();
        var protoItem = putItem.getItem();
        var tableName = itemRequest.getTable();
        Map<String, Object> attributes = ProtoUtils.structToMap(protoItem.getAttributes());
        var key = new Key(protoItem.getKey().getPartitionKey(), protoItem.getKey().getSortKey());
        var item = new Item(key, attributes);
        roxDB.putItem(tableName, item);
        responseObserver.onNext(ItemResponse.newBuilder().setCorrelationId(itemRequest.getCorrelationId()).setPutItemResponse(ItemResponse.PutItemResponse.newBuilder().setKey(protoItem.getKey()).build()).build());
    }

    private void getItem(StreamObserver<ItemResponse> responseObserver, ItemRequest itemRequest) throws RocksDBException {
        String tableName = itemRequest.getTable();
        var getItemRequest = itemRequest.getGetItem();
        var key = new Key(getItemRequest.getKey().getPartitionKey(), getItemRequest.getKey().getSortKey());
        var item = roxDB.getItem(tableName, key);
        if (item == null) {
            responseObserver.onNext(ItemResponse.newBuilder().setCorrelationId(itemRequest.getCorrelationId()).setGetItemResponse(ItemResponse.GetItemResponse.newBuilder().setItemNotFound(ItemResponse.GetItemResponse.ItemNotFound.newBuilder().setKey(getItemRequest.getKey()).build()).build()).build());
            return;
        }
        var protoItem = ProtoUtils.itemToProto(item);
        responseObserver.onNext(ItemResponse.newBuilder().setCorrelationId(itemRequest.getCorrelationId()).setGetItemResponse(ItemResponse.GetItemResponse.newBuilder().setItem(protoItem).build()).build());
    }

    private void deleteItem(StreamObserver<ItemResponse> responseObserver, ItemRequest itemRequest) throws RocksDBException {
        String tableName = itemRequest.getTable();
        var deleteItemRequest = itemRequest.getDeleteItem();
        var key = new Key(deleteItemRequest.getKey().getPartitionKey(), deleteItemRequest.getKey().getSortKey());
        roxDB.deleteItem(tableName, key);
        responseObserver.onNext(ItemResponse.newBuilder().setCorrelationId(itemRequest.getCorrelationId()).setDeleteItemResponse(ItemResponse.DeleteItemResponse.newBuilder().setKey(deleteItemRequest.getKey()).build()).build());
    }

    private void query(StreamObserver<ItemResponse> responseObserver, ItemRequest itemRequest) throws RocksDBException {
        String tableName = itemRequest.getTable();
        var queryItemsRequest = itemRequest.getQuery();
        var items = roxDB.query(tableName, queryItemsRequest.getPartitionKey(), Optional.of(queryItemsRequest.getSortKeyStart()), Optional.of(queryItemsRequest.getSortKeyEnd()));
        var itemsQueryResultBuilder = ItemResponse.QueryResponse.ItemsQueryResult.newBuilder();
        for (var item : items) {
            var protoItem = ProtoUtils.itemToProto(item);
            itemsQueryResultBuilder.addItems(protoItem);
        }
        responseObserver.onNext(ItemResponse.newBuilder().setCorrelationId(itemRequest.getCorrelationId()).setQueryResponse(ItemResponse.QueryResponse.newBuilder().setItemsQueryResult(itemsQueryResultBuilder.build()).build()).build());
    }

}
