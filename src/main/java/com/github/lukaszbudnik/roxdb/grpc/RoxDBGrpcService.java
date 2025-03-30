package com.github.lukaszbudnik.roxdb.grpc;

import com.github.lukaszbudnik.roxdb.core.RoxDB;
import com.github.lukaszbudnik.roxdb.proto.*;
import com.github.lukaszbudnik.roxdb.proto.Error;
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
    public StreamObserver<PutItemRequest> putItem(StreamObserver<PutItemResponse> responseObserver) {
        return new StreamObserver<PutItemRequest>() {
            @Override
            public void onNext(PutItemRequest putItemRequest) {
                try {
                    var protoItem = putItemRequest.getItem();
                    Map<String, Object> attributes = ProtoUtils.structToMap(protoItem.getAttributes());
                    var key = new com.github.lukaszbudnik.roxdb.core.Key(protoItem.getKey().getPartitionKey(), protoItem.getKey().getSortKey());
                    var item = new com.github.lukaszbudnik.roxdb.core.Item(key, attributes);
                    roxDB.putItem(putItemRequest.getTableName(), item);
                    responseObserver.onNext(
                        PutItemResponse.newBuilder()
                                .setCorrelationId(putItemRequest.getCorrelationId())
                                .setSuccess(
                                    Success.newBuilder().build()
                                ).build()
                    );
                } catch (RocksDBException e) {
                    responseObserver.onNext(
                            PutItemResponse.newBuilder()
                                    .setCorrelationId(putItemRequest.getCorrelationId())
                                    .setError(
                                            Error.newBuilder()
                                                    .setMessage(e.getMessage())
                                                    .setCode(ROCKS_DB_ERROR)
                                    .build()
                            ).build()
                    );
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

    @Override
    public StreamObserver<GetItemRequest> getItem(StreamObserver<GetItemResponse> responseObserver) {
        return new StreamObserver<GetItemRequest>() {
            @Override
            public void onNext(GetItemRequest getItemRequest) {
                try {
                    String tableName = getItemRequest.getTableName();
                    var key = new com.github.lukaszbudnik.roxdb.core.Key(getItemRequest.getKey().getPartitionKey(), getItemRequest.getKey().getSortKey());
                    var item = roxDB.getItem(tableName, key);
                    if (item == null) {
                        responseObserver.onNext(GetItemResponse.newBuilder().setCorrelationId(getItemRequest.getCorrelationId()).setNotFound(GetItemResponse.ItemNotFound.newBuilder().build()).build());
                        return;
                    }
                    var protoItem = ProtoUtils.itemToProto(item);
                    responseObserver.onNext(GetItemResponse.newBuilder().setCorrelationId(getItemRequest.getCorrelationId()).setItem(protoItem).build());
                } catch (RocksDBException e) {
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

}
