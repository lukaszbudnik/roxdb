package com.github.lukaszbudnik.roxdb.grpc;

import com.github.lukaszbudnik.roxdb.db.RoxDBImpl;
import com.github.lukaszbudnik.roxdb.v1.*;
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
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

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

        server = InProcessServerBuilder.forName(serverName).directExecutor().addService(new RoxDBGrpcService(roxDB)).build().start();

        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

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
            ItemRequest putRequest = ItemRequest.newBuilder().setCorrelationId(putItemId.toString()).setTable(tableName).setPutItem(ItemRequest.PutItem.newBuilder().setItem(createTestItem(partitionKey, sortKey, attributes)).build()).build();
            requestObserver.onNext(putRequest);

            // Send GetItem request
            Key key = Key.newBuilder().setPartitionKey(partitionKey).setSortKey(sortKey).build();
            ItemRequest getRequest = ItemRequest.newBuilder().setCorrelationId(getItemId.toString()).setTable(tableName).setGetItem(ItemRequest.GetItem.newBuilder().setKey(key).build()).build();
            requestObserverGetItem.onNext(getRequest);

            // Send DeleteItem request
            ItemRequest deleteRequest = ItemRequest.newBuilder().setCorrelationId(deleteItemId.toString()).setTable(tableName).setDeleteItem(ItemRequest.DeleteItem.newBuilder().setKey(key).build()).build();
            requestObserver.onNext(deleteRequest);

            // Seng GetItem request for non-existing deleted item
            ItemRequest getRequestNotFound = ItemRequest.newBuilder().setCorrelationId(getItemNotFoundId.toString()).setTable(tableName).setGetItem(ItemRequest.GetItem.newBuilder().setKey(key).build()).build();
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
            assertTrue(putItemResponse.hasPutItemResponse(), "Expected to get put item response for " + putItemId);
            assertEquals(putItemId.toString(), putItemResponse.getCorrelationId());
            assertEquals(key, putItemResponse.getPutItemResponse().getKey());

            // Verify GetItem
            ItemResponse receivedItem = receivedItems.get(getItemId.toString());
            assertTrue(receivedItem.hasGetItemResponse(), "Expected to get received item for " + getItemId);
            // Verify Key
            assertEquals(key, receivedItem.getGetItemResponse().getItem().getKey());
            // Verify attributes
            Struct receivedAttributes = receivedItem.getGetItemResponse().getItem().getAttributes();
            assertEquals("value1", receivedAttributes.getFieldsOrThrow("field1").getStringValue());
            assertEquals(123.0, receivedAttributes.getFieldsOrThrow("field2").getNumberValue());
            assertTrue(receivedAttributes.getFieldsOrThrow("field3").getBoolValue());

            // Verity DeleteItem
            ItemResponse deleteItemResponse = putDeleteItemResponses.get(deleteItemId.toString());
            assertTrue(deleteItemResponse.hasDeleteItemResponse(), "Expected to get delete item response for " + deleteItemId);
            assertEquals(deleteItemId.toString(), deleteItemResponse.getCorrelationId());
            assertEquals(key, deleteItemResponse.getDeleteItemResponse().getKey());

            // Verify GetItem - not found
            ItemResponse notFoundItem = receivedItems.get(getItemNotFoundId.toString());
            assertTrue(notFoundItem.getGetItemResponse().hasItemNotFound(), "Expected to get item not found for " + getItemNotFoundId);
            assertEquals(getItemNotFoundId.toString(), notFoundItem.getCorrelationId());
            assertEquals(key, notFoundItem.getGetItemResponse().getItemNotFound().getKey());
        } catch (Exception e) {
            requestObserver.onError(e);
            throw e;
        }
    }

    @Test
    public void testQuery() throws Exception {
        // Setup test data
        String partitionKey = "test-partition";
        String sortKeyPrefix = "sort-key-";
        int numberOfItems = 5;
        int numberOfQueryItems = 3;
        List<UUID> itemIds = new ArrayList<>();
        Map<String, ItemResponse> putItemResponses = new HashMap<>();
        Map<String, ItemResponse> queryResponses = new HashMap<>();

        // Create and store test items
        StreamObserver<ItemRequest> requestObserver = asyncStub.processItems(new StreamObserver<ItemResponse>() {
            @Override
            public void onNext(ItemResponse response) {
                if (response.hasPutItemResponse()) {
                    // Store put item responses
                    putItemResponses.put(response.getCorrelationId(), response);
                }
                if (response.hasQueryResponse()) {
                    // Store query responses
                    queryResponses.put(response.getCorrelationId(), response);
                }
            }

            @Override
            public void onError(Throwable t) {
                fail("Unexpected error: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                // Do nothing
            }
        });

        try {
            // Insert test items
            for (int i = 0; i < numberOfItems; i++) {
                UUID itemId = UUID.randomUUID();
                itemIds.add(itemId);

                Map<String, Object> attributes = new HashMap<>();
                attributes.put("data", "test-data-" + i);

                Item item = createTestItem(partitionKey, sortKeyPrefix + i, attributes);

                ItemRequest putRequest = ItemRequest.newBuilder().setCorrelationId(itemId.toString()).setPutItem(ItemRequest.PutItem.newBuilder().setItem(item).build()).build();

                requestObserver.onNext(putRequest);
            }

            // Perform query
            UUID queryId = UUID.randomUUID();
            // sort keys are inclusive so this will be 3 items: 0, 1, 2
            String sortKeyStart = sortKeyPrefix + "0";
            String sortKeyEnd = sortKeyPrefix + "2";
            ItemRequest.Query queryRequest = ItemRequest.Query.newBuilder().setPartitionKey(partitionKey).setSortKeyStart(sortKeyStart).setSortKeyEnd(sortKeyEnd).build();

            ItemRequest request = ItemRequest.newBuilder().setCorrelationId(queryId.toString()).setQuery(queryRequest).build();

            requestObserver.onNext(request);
            requestObserver.onCompleted();

            // Verify results
            // Verify all put operations succeeded
            assertEquals(numberOfItems, putItemResponses.size(), "Expected " + numberOfItems + " put responses");
            for (UUID itemId : itemIds) {
                ItemResponse putResponse = putItemResponses.get(itemId.toString());
                assertNotNull(putResponse, "Expected response for item " + itemId);
                assertTrue(putResponse.hasPutItemResponse(), "Expected success response for item " + itemId);
                assertEquals(itemId.toString(), putResponse.getCorrelationId());
                assertEquals(partitionKey, putResponse.getPutItemResponse().getKey().getPartitionKey());
            }

            // Verify query response
            ItemResponse queryResponse = queryResponses.get(queryId.toString());
            assertNotNull(queryResponse, "Expected query response");
            assertTrue(queryResponse.hasQueryResponse(), "Expected query result");

            ItemResponse.QueryResponse queryResult = queryResponse.getQueryResponse();
            assertEquals(numberOfQueryItems, queryResult.getItemsQueryResult().getItemsCount(), "Expected to receive " + numberOfQueryItems + " inserted items");

            // Verify query results contain all inserted items
            Set<String> returnedSortKeys = queryResult.getItemsQueryResult().getItemsList().stream().map(item -> item.getKey().getSortKey()).collect(Collectors.toSet());

            for (int i = 0; i < numberOfQueryItems; i++) {
                String expectedSortKey = sortKeyPrefix + i;
                assertTrue(returnedSortKeys.contains(expectedSortKey), "Expected to find item with sort key: " + expectedSortKey);
            }

        } catch (Exception e) {
            requestObserver.onError(e);
            throw e;
        }
    }


    private Item createTestItem(String partitionKey, String sortKey, Map<String, Object> attributes) {
        Key key = Key.newBuilder().setPartitionKey(partitionKey).setSortKey(sortKey).build();

        return Item.newBuilder().setKey(key).setAttributes(ProtoUtils.mapToStruct(attributes)).build();
    }

}
