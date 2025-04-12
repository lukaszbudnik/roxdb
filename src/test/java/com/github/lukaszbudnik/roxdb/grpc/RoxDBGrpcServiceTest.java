package com.github.lukaszbudnik.roxdb.grpc;

import com.github.lukaszbudnik.roxdb.core.RoxDBImpl;
import com.github.lukaszbudnik.roxdb.proto.*;
import com.google.protobuf.Struct;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RoxDBGrpcServiceTest {
    private Server server;
    private ManagedChannel channel;
    private RoxDBGrpc.RoxDBStub asyncStub;
    private RoxDBImpl roxDB;
    private String dbPath;

    @BeforeEach
    void setUp() throws IOException, RocksDBException {
        String serverName = InProcessServerBuilder.generateName();
        dbPath = "/tmp/roxdb-test-" + System.currentTimeMillis();
        roxDB = new RoxDBImpl(dbPath);

        server = InProcessServerBuilder
                .forName(serverName)
                .directExecutor()
                .addService(new RoxDBGrpcService(roxDB))
                .build()
                .start();

        channel = InProcessChannelBuilder
                .forName(serverName)
                .directExecutor()
                .build();

        asyncStub = RoxDBGrpc.newStub(channel);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        roxDB.close();
        FileUtils.deleteQuietly(new File(dbPath));
    }

    @Test
    void crud() throws InterruptedException {
        // 2 GetItem operations
        CountDownLatch latch = new CountDownLatch(2);
        Map<String, ItemResponse> putDeleteItemResponses = new HashMap<>();
        Map<String, ItemResponse> receivedItems = new HashMap<>();
        List<Throwable> errors = new ArrayList<>();

        UUID putItemId = UUID.randomUUID();
        UUID getItemId = UUID.randomUUID();
        UUID deleteItemId = UUID.randomUUID();
        UUID getItemNotFoundId = UUID.randomUUID();

        // TODO
        // 1. update operation + 2nd get

        // Create test data
        String tableName = "test-table";
        String partitionKey = "pk1";
        String sortKey = "sk1";

        Map<String, Object> attributes = new HashMap<>();
        attributes.put("field1", "value1");
        attributes.put("field2", 123);
        attributes.put("field3", true);

        // Setup streaming observer for receiving responses
        StreamObserver<ItemResponse> responseObserver = new StreamObserver<ItemResponse>() {
            @Override
            public void onNext(ItemResponse response) {
                putDeleteItemResponses.put(response.getCorrelationId(), response);
            }

            @Override
            public void onError(Throwable t) {
                errors.add(t);
            }

            @Override
            public void onCompleted() {
            }
        };

        // Start bidirectional streaming
        StreamObserver<ItemRequest> requestObserver = asyncStub.processItems(responseObserver);

        StreamObserver<ItemResponse> responseObserverGetItem = new StreamObserver<ItemResponse>() {
            @Override
            public void onNext(ItemResponse response) {
                receivedItems.put(response.getCorrelationId(), response);
                latch.countDown();
            }

            @Override
            public void onError(Throwable t) {
                errors.add(t);
            }

            @Override
            public void onCompleted() {
            }
        };

        // Start bidirectional streaming
        StreamObserver<ItemRequest> requestObserverGetItem = asyncStub.processItems(responseObserverGetItem);

        try {
            // Send PutItem request
            ItemRequest putRequest = ItemRequest.newBuilder()
                    .setCorrelationId(putItemId.toString())
                    .setTableName(tableName)
                    .setPutItem(ItemRequest.PutItem.newBuilder()
                            .setItem(createTestItem(partitionKey, sortKey, attributes)
                            ).build()
                    ).build();
            requestObserver.onNext(putRequest);

            // Send GetItem request
            ItemRequest getRequest = ItemRequest.newBuilder()
                    .setCorrelationId(getItemId.toString())
                    .setTableName(tableName)
                    .setGetItem(ItemRequest.GetItem.newBuilder()
                            .setKey(Key.newBuilder()
                                    .setPartitionKey(partitionKey)
                                    .setSortKey(sortKey)
                                    .build()
                            ).build()

                    )
                    .build();
            requestObserverGetItem.onNext(getRequest);

            // Send DeleteItem request
            ItemRequest deleteRequest = ItemRequest.newBuilder()
                    .setCorrelationId(deleteItemId.toString())
                    .setTableName(tableName)
                    .setDeleteItem(ItemRequest.DeleteItem.newBuilder()
                            .setKey(Key.newBuilder()
                                    .setPartitionKey(partitionKey)
                                    .setSortKey(sortKey)
                                    .build()
                            ).build()
                    ).build();
            requestObserver.onNext(deleteRequest);

            // Seng GetItem request for non-existing deleted item
            ItemRequest getRequestNotFound = ItemRequest.newBuilder()
                    .setCorrelationId(getItemNotFoundId.toString())
                    .setTableName(tableName)
                    .setGetItem(ItemRequest.GetItem.newBuilder()
                            .setKey(Key.newBuilder()
                                    .setPartitionKey(partitionKey)
                                    .setSortKey(sortKey)
                                    .build()
                            ).build()
                    ).build();
            requestObserverGetItem.onNext(getRequestNotFound);

            // Complete the request stream
            requestObserver.onCompleted();
            requestObserverGetItem.onCompleted();

            // Wait for response with timeout
            assertTrue(latch.await(5, TimeUnit.SECONDS), "Timeout waiting for response");

            // Verify results
            assertTrue(errors.isEmpty(), "Unexpected errors: " + errors);

            // Verify PutItem
            ItemResponse putItemResponse = putDeleteItemResponses.get(putItemId.toString());
            assertEquals(putItemId.toString(), putItemResponse.getCorrelationId());
            assertTrue(putItemResponse.hasSuccess());

            // Verify GetItem
            ItemResponse receivedItem = receivedItems.get(getItemId.toString());
            assertTrue(receivedItem.hasItemResult(), "Expected to get received item for " + getItemId);
            // Verify Key
            assertEquals(partitionKey, receivedItem.getItemResult().getItem().getKey().getPartitionKey());
            assertEquals(sortKey, receivedItem.getItemResult().getItem().getKey().getSortKey());
            // Verify attributes
            Struct receivedAttributes = receivedItem.getItemResult().getItem().getAttributes();
            assertEquals("value1", receivedAttributes.getFieldsOrThrow("field1").getStringValue());
            assertEquals(123.0, receivedAttributes.getFieldsOrThrow("field2").getNumberValue());
            assertTrue(receivedAttributes.getFieldsOrThrow("field3").getBoolValue());

            // Verity DeleteItem
            ItemResponse deleteItemResponse = putDeleteItemResponses.get(deleteItemId.toString());
            assertEquals(deleteItemId.toString(), deleteItemResponse.getCorrelationId());
            assertTrue(deleteItemResponse.hasSuccess());

            // Verify GetItem - not found
            ItemResponse notFoundItem = receivedItems.get(getItemNotFoundId.toString());
            assertTrue(notFoundItem.hasNotFound(), "Expected to not get receive item for " + getItemNotFoundId);
        } catch (Exception e) {
            requestObserver.onError(e);
            throw e;
        }
    }

    private Item createTestItem(String partitionKey, String sortKey, Map<String, Object> attributes) {
        Key key = Key.newBuilder()
                .setPartitionKey(partitionKey)
                .setSortKey(sortKey)
                .build();

        return Item.newBuilder()
                .setKey(key)
                .setAttributes(ProtoUtils.mapToStruct(attributes))
                .build();
    }

}
