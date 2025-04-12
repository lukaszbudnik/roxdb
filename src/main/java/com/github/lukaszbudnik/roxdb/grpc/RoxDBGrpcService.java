package com.github.lukaszbudnik.roxdb.grpc;

import com.github.lukaszbudnik.roxdb.core.RoxDB;
import com.github.lukaszbudnik.roxdb.proto.ItemRequest;
import com.github.lukaszbudnik.roxdb.proto.ItemResponse;
import com.github.lukaszbudnik.roxdb.proto.ItemResponse.Success;
import com.github.lukaszbudnik.roxdb.proto.RoxDBGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.rocksdb.RocksDBException;

import java.util.Map;

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
        var tableName = itemRequest.getTableName();
        Map<String, Object> attributes = ProtoUtils.structToMap(protoItem.getAttributes());
        var key = new com.github.lukaszbudnik.roxdb.core.Key(protoItem.getKey().getPartitionKey(), protoItem.getKey().getSortKey());
        var item = new com.github.lukaszbudnik.roxdb.core.Item(key, attributes);
        roxDB.putItem(tableName, item);
        responseObserver.onNext(ItemResponse.newBuilder().setCorrelationId(itemRequest.getCorrelationId()).setSuccess(Success.newBuilder().build()).build());
    }

    private void getItem(StreamObserver<ItemResponse> responseObserver, ItemRequest itemRequest) throws RocksDBException {
        String tableName = itemRequest.getTableName();
        var getItemRequest = itemRequest.getGetItem();
        var key = new com.github.lukaszbudnik.roxdb.core.Key(getItemRequest.getKey().getPartitionKey(), getItemRequest.getKey().getSortKey());
        var item = roxDB.getItem(tableName, key);
        if (item == null) {
            responseObserver.onNext(ItemResponse.newBuilder().setCorrelationId(itemRequest.getCorrelationId()).setNotFound(ItemResponse.ItemNotFound.newBuilder().build()).build());
            return;
        }
        var protoItem = ProtoUtils.itemToProto(item);
        responseObserver.onNext(ItemResponse.newBuilder().setCorrelationId(itemRequest.getCorrelationId()).setItemResult(ItemResponse.ItemResult.newBuilder().setItem(protoItem).build()).build());
    }

    private void deleteItem(StreamObserver<ItemResponse> responseObserver, ItemRequest itemRequest) throws RocksDBException {
        String tableName = itemRequest.getTableName();
        var deleteItemRequest = itemRequest.getDeleteItem();
        var key = new com.github.lukaszbudnik.roxdb.core.Key(deleteItemRequest.getKey().getPartitionKey(), deleteItemRequest.getKey().getSortKey());
        roxDB.deleteItem(tableName, key);
        responseObserver.onNext(ItemResponse.newBuilder().setCorrelationId(itemRequest.getCorrelationId()).setSuccess(Success.newBuilder().build()).build());
    }

}
