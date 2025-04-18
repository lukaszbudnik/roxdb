package com.github.lukaszbudnik.roxdb.grpc;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.github.lukaszbudnik.roxdb.rocksdb.RoxDB;
import com.github.lukaszbudnik.roxdb.v1.*;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.rocksdb.RocksDBException;

@ExtendWith(MockitoExtension.class)
class RoxDBGrpcServiceTest {
  private Server server;
  private ManagedChannel channel;
  private RoxDBGrpc.RoxDBStub asyncStub;
  @Mock private RoxDB roxDB;
  @Captor private ArgumentCaptor<com.github.lukaszbudnik.roxdb.rocksdb.Item> itemCaptor;
  @Captor private ArgumentCaptor<com.github.lukaszbudnik.roxdb.rocksdb.Key> keyCaptor;

  @BeforeEach
  void setUp() throws IOException, RocksDBException {
    String serverName = InProcessServerBuilder.generateName();

    server =
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(new RoxDBGrpcService(roxDB))
            .build()
            .start();

    channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();

    asyncStub = RoxDBGrpc.newStub(channel);
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
  }

  @Test
  void putItem() throws RocksDBException, InterruptedException {
    UUID putItemId = UUID.randomUUID();
    String table = "table";
    String partitionKey = "pk1";
    String sortKey = "sk1";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("field1", "value1");
    attributes.put("field2", 123.0);
    attributes.put("field3", true);

    CountDownLatch latch = new CountDownLatch(1);
    Map<String, ItemResponse> responses = new HashMap<>();

    // putItem is a void method
    doNothing()
        .when(roxDB)
        .putItem(eq(table), any(com.github.lukaszbudnik.roxdb.rocksdb.Item.class));

    ItemRequest putItemRequest =
        ItemRequest.newBuilder()
            .setCorrelationId(putItemId.toString())
            .setPutItem(
                ItemRequest.PutItem.newBuilder()
                    .setTable(table)
                    .setItem(
                        Item.newBuilder()
                            .setKey(
                                Key.newBuilder()
                                    .setPartitionKey(partitionKey)
                                    .setSortKey(sortKey)
                                    .build())
                            .setAttributes(ProtoUtils.mapToStruct(attributes))
                            .build())
                    .build())
            .build();

    var responseObserver =
        new StreamObserver<ItemResponse>() {
          @Override
          public void onNext(ItemResponse itemResponse) {
            responses.put(itemResponse.getCorrelationId(), itemResponse);
            latch.countDown();
          }

          @Override
          public void onError(Throwable throwable) {
            fail("onError should not be called");
          }

          @Override
          public void onCompleted() {
            // no-op
          }
        };

    StreamObserver<ItemRequest> requestObserver = asyncStub.processItems(responseObserver);
    requestObserver.onNext(putItemRequest);
    requestObserver.onCompleted();

    assertTrue(latch.await(1, TimeUnit.SECONDS));

    // verify Mocks were called
    verify(roxDB).putItem(eq(table), itemCaptor.capture());
    var capturedItem = itemCaptor.getValue();
    assertEquals(partitionKey, capturedItem.key().partitionKey());
    assertEquals(sortKey, capturedItem.key().sortKey());
    assertEquals(attributes, capturedItem.attributes());

    // verify expected gRPC response
    ItemResponse putItemResponse = responses.get(putItemId.toString());
    assertTrue(
        putItemResponse.hasPutItemResponse(), "Expected to get put item response for " + putItemId);
    assertEquals(putItemId.toString(), putItemResponse.getCorrelationId());
    assertEquals(partitionKey, putItemResponse.getPutItemResponse().getKey().getPartitionKey());
    assertEquals(sortKey, putItemResponse.getPutItemResponse().getKey().getSortKey());
  }

  @Test
  void updateItem() throws RocksDBException, InterruptedException {
    // Test UpdateItem
    UUID updateItemId = UUID.randomUUID();
    String table = "table";
    String partitionKey = "pk1";
    String sortKey = "sk1";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("field1", "value1");
    attributes.put("field2", 123.0);
    attributes.put("field3", true);
    ItemRequest updateItemRequest =
        ItemRequest.newBuilder()
            .setCorrelationId(updateItemId.toString())
            .setUpdateItem(
                ItemRequest.UpdateItem.newBuilder()
                    .setTable(table)
                    .setItem(
                        Item.newBuilder()
                            .setKey(
                                Key.newBuilder()
                                    .setPartitionKey(partitionKey)
                                    .setSortKey(sortKey)
                                    .build())
                            .setAttributes(ProtoUtils.mapToStruct(attributes))
                            .build())
                    .build())
            .build();

    CountDownLatch latch = new CountDownLatch(1);
    Map<String, ItemResponse> responses = new HashMap<>();

    // updateItem is a void method
    doNothing()
        .when(roxDB)
        .updateItem(eq(table), any(com.github.lukaszbudnik.roxdb.rocksdb.Item.class));

    StreamObserver<ItemResponse> responseObserver =
        new StreamObserver<ItemResponse>() {
          @Override
          public void onNext(ItemResponse itemResponse) {
            responses.put(itemResponse.getCorrelationId(), itemResponse);
            latch.countDown();
          }

          @Override
          public void onError(Throwable throwable) {
            fail("onError should not be called");
          }

          @Override
          public void onCompleted() {
            // no-op
          }
        };
    StreamObserver<ItemRequest> requestObserver = asyncStub.processItems(responseObserver);
    requestObserver.onNext(updateItemRequest);
    requestObserver.onCompleted();

    assertTrue(latch.await(1, TimeUnit.SECONDS));

    // verify Mocks were called
    verify(roxDB).updateItem(eq(table), itemCaptor.capture());
    var capturedItem = itemCaptor.getValue();
    assertEquals(partitionKey, capturedItem.key().partitionKey());
    assertEquals(sortKey, capturedItem.key().sortKey());
    assertEquals(attributes, capturedItem.attributes());

    // verify expected gRPC response
    ItemResponse updateItemResponse = responses.get(updateItemId.toString());
    assertTrue(
        updateItemResponse.hasUpdateItemResponse(),
        "Expected to get update item response for " + updateItemId);
    assertEquals(updateItemId.toString(), updateItemResponse.getCorrelationId());
    assertEquals(
        partitionKey, updateItemResponse.getUpdateItemResponse().getKey().getPartitionKey());
    assertEquals(sortKey, updateItemResponse.getUpdateItemResponse().getKey().getSortKey());
  }

  @Test
  void deleteItem() throws RocksDBException, InterruptedException {
    // Test DeleteItem
    UUID deleteItemId = UUID.randomUUID();
    String table = "table";
    String partitionKey = "pk1";
    String sortKey = "sk1";
    ItemRequest deleteItemRequest =
        ItemRequest.newBuilder()
            .setCorrelationId(deleteItemId.toString())
            .setDeleteItem(
                ItemRequest.DeleteItem.newBuilder()
                    .setTable(table)
                    .setKey(
                        Key.newBuilder().setPartitionKey(partitionKey).setSortKey(sortKey).build())
                    .build())
            .build();

    CountDownLatch latch = new CountDownLatch(1);
    Map<String, ItemResponse> responses = new HashMap<>();

    // deleteItem is a void method
    doNothing()
        .when(roxDB)
        .deleteItem(eq(table), any(com.github.lukaszbudnik.roxdb.rocksdb.Key.class));

    StreamObserver<ItemResponse> responseObserver =
        new StreamObserver<ItemResponse>() {
          @Override
          public void onNext(ItemResponse itemResponse) {
            responses.put(itemResponse.getCorrelationId(), itemResponse);
            latch.countDown();
          }

          @Override
          public void onError(Throwable throwable) {
            fail("onError should not be called");
          }

          @Override
          public void onCompleted() {
            // no-op
          }
        };
    StreamObserver<ItemRequest> requestObserver = asyncStub.processItems(responseObserver);
    requestObserver.onNext(deleteItemRequest);
    requestObserver.onCompleted();

    assertTrue(latch.await(1, TimeUnit.SECONDS));

    // verify Mocks were called
    verify(roxDB).deleteItem(eq(table), keyCaptor.capture());
    var capturedKey = keyCaptor.getValue();
    assertEquals(partitionKey, capturedKey.partitionKey());
    assertEquals(sortKey, capturedKey.sortKey());

    // verify expected gRPC response
    ItemResponse deleteItemResponse = responses.get(deleteItemId.toString());
    assertTrue(
        deleteItemResponse.hasDeleteItemResponse(),
        "Expected to get delete item response for " + deleteItemId);
    assertEquals(deleteItemId.toString(), deleteItemResponse.getCorrelationId());
    assertEquals(
        partitionKey, deleteItemResponse.getDeleteItemResponse().getKey().getPartitionKey());
    assertEquals(sortKey, deleteItemResponse.getDeleteItemResponse().getKey().getSortKey());
  }

  @Test
  void getItem() throws RocksDBException, InterruptedException {
    // Test GetItem
    UUID getItemId = UUID.randomUUID();
    String table = "table";
    String partitionKey = "pk1";
    String sortKey = "sk1";
    ItemRequest getItemRequest =
        ItemRequest.newBuilder()
            .setCorrelationId(getItemId.toString())
            .setGetItem(
                ItemRequest.GetItem.newBuilder()
                    .setTable(table)
                    .setKey(
                        Key.newBuilder().setPartitionKey(partitionKey).setSortKey(sortKey).build())
                    .build())
            .build();

    CountDownLatch latch = new CountDownLatch(1);
    Map<String, ItemResponse> responses = new HashMap<>();

    // getItem is a void method
    // create sample item
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("field1", "value1");
    attributes.put("field2", 123.0);
    attributes.put("field3", true);
    var item =
        new com.github.lukaszbudnik.roxdb.rocksdb.Item(
            new com.github.lukaszbudnik.roxdb.rocksdb.Key(partitionKey, sortKey), attributes);
    when(roxDB.getItem(eq(table), any(com.github.lukaszbudnik.roxdb.rocksdb.Key.class)))
        .thenReturn(item);

    StreamObserver<ItemResponse> responseObserver =
        new StreamObserver<ItemResponse>() {
          @Override
          public void onNext(ItemResponse itemResponse) {
            responses.put(getItemId.toString(), itemResponse);
            latch.countDown();
          }

          @Override
          public void onError(Throwable throwable) {
            fail("onError should not be called");
          }

          @Override
          public void onCompleted() {
            // no-op
          }
        };
    StreamObserver<ItemRequest> requestObserver = asyncStub.processItems(responseObserver);
    requestObserver.onNext(getItemRequest);
    requestObserver.onCompleted();

    assertTrue(latch.await(1, TimeUnit.SECONDS));

    // verify Mocks were called
    verify(roxDB).getItem(eq(table), keyCaptor.capture());
    var capturedKey = keyCaptor.getValue();
    assertEquals(partitionKey, capturedKey.partitionKey());
    assertEquals(sortKey, capturedKey.sortKey());

    // verify expected gRPC response
    ItemResponse getItemResponse = responses.get(getItemId.toString());
    assertTrue(
        getItemResponse.hasGetItemResponse(), "Expected to get get item response for " + getItemId);
    assertEquals(getItemId.toString(), getItemResponse.getCorrelationId());
    assertEquals(
        partitionKey, getItemResponse.getGetItemResponse().getItem().getKey().getPartitionKey());
    assertEquals(sortKey, getItemResponse.getGetItemResponse().getItem().getKey().getSortKey());
    assertEquals(
        attributes,
        ProtoUtils.structToMap(getItemResponse.getGetItemResponse().getItem().getAttributes()));
  }

  @Test
  void getItemNotFound() throws RocksDBException, InterruptedException {
    // Test GetItem
    UUID getItemId = UUID.randomUUID();
    String table = "table";
    String partitionKey = "pk1";
    String sortKey = "sk1";
    ItemRequest getItemRequest =
        ItemRequest.newBuilder()
            .setCorrelationId(getItemId.toString())
            .setGetItem(
                ItemRequest.GetItem.newBuilder()
                    .setTable(table)
                    .setKey(
                        Key.newBuilder().setPartitionKey(partitionKey).setSortKey(sortKey).build())
                    .build())
            .build();

    CountDownLatch latch = new CountDownLatch(1);
    Map<String, ItemResponse> responses = new HashMap<>();

    // getItem returns null
    when(roxDB.getItem(eq(table), any(com.github.lukaszbudnik.roxdb.rocksdb.Key.class)))
        .thenReturn(null);

    StreamObserver<ItemResponse> responseObserver =
        new StreamObserver<ItemResponse>() {
          @Override
          public void onNext(ItemResponse itemResponse) {
            responses.put(getItemId.toString(), itemResponse);
            latch.countDown();
          }

          @Override
          public void onError(Throwable throwable) {
            fail("onError should not be called");
          }

          @Override
          public void onCompleted() {
            // no-op
          }
        };
    StreamObserver<ItemRequest> requestObserver = asyncStub.processItems(responseObserver);
    requestObserver.onNext(getItemRequest);
    requestObserver.onCompleted();

    assertTrue(latch.await(1, TimeUnit.SECONDS));

    // verify Mocks were called
    verify(roxDB).getItem(eq(table), keyCaptor.capture());
    var capturedKey = keyCaptor.getValue();
    assertEquals(partitionKey, capturedKey.partitionKey());
    assertEquals(sortKey, capturedKey.sortKey());

    // verify expected gRPC response
    ItemResponse getItemResponse = responses.get(getItemId.toString());
    assertTrue(
        getItemResponse.hasGetItemResponse(), "Expected to get get item response for " + getItemId);
    assertEquals(getItemId.toString(), getItemResponse.getCorrelationId());
    assertEquals(
        partitionKey,
        getItemResponse.getGetItemResponse().getItemNotFound().getKey().getPartitionKey());
    assertEquals(
        sortKey, getItemResponse.getGetItemResponse().getItemNotFound().getKey().getSortKey());
  }

  @Test
  void query() throws Exception {
    // Setup test data
    String table = "table";
    String partitionKey = "test-partition";
    String sortKeyPrefix = "sort-key-";
    int numberOfQueryItems = 3;

    Map<String, ItemResponse> responses = new HashMap<>();
    CountDownLatch latch = new CountDownLatch(1);

    // setup RoxDB.query mock to return 3 items
    when(roxDB.query(
            eq(table),
            eq(partitionKey),
            eq(Optional.of(sortKeyPrefix + "0")),
            eq(Optional.of(sortKeyPrefix + "2"))))
        .thenReturn(
            List.of(
                new com.github.lukaszbudnik.roxdb.rocksdb.Item(
                    new com.github.lukaszbudnik.roxdb.rocksdb.Key(
                        partitionKey, sortKeyPrefix + "0"),
                    Map.of("field", "value1")),
                new com.github.lukaszbudnik.roxdb.rocksdb.Item(
                    new com.github.lukaszbudnik.roxdb.rocksdb.Key(
                        partitionKey, sortKeyPrefix + "1"),
                    Map.of("field", "value2")),
                new com.github.lukaszbudnik.roxdb.rocksdb.Item(
                    new com.github.lukaszbudnik.roxdb.rocksdb.Key(
                        partitionKey, sortKeyPrefix + "2"),
                    Map.of("field", "value3"))));

    // Create and store test items
    StreamObserver<ItemRequest> requestObserver =
        asyncStub.processItems(
            new StreamObserver<ItemResponse>() {
              @Override
              public void onNext(ItemResponse response) {
                responses.put(response.getCorrelationId(), response);
                latch.countDown();
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

    // Perform query
    UUID queryId = UUID.randomUUID();
    // sort keys are inclusive so this will be 3 items: 0, 1, 2
    String sortKeyStart = sortKeyPrefix + "0";
    String sortKeyEnd = sortKeyPrefix + "2";
    ItemRequest.Query queryRequest =
        ItemRequest.Query.newBuilder()
            .setTable(table)
            .setPartitionKey(partitionKey)
            .setSortKeyStart(sortKeyStart)
            .setSortKeyEnd(sortKeyEnd)
            .build();

    ItemRequest request =
        ItemRequest.newBuilder()
            .setCorrelationId(queryId.toString())
            .setQuery(queryRequest)
            .build();

    requestObserver.onNext(request);
    requestObserver.onCompleted();

    assertTrue(latch.await(1, TimeUnit.SECONDS));

    verify(roxDB)
        .query(
            eq(table),
            eq(partitionKey),
            eq(Optional.of(sortKeyStart)),
            eq(Optional.of(sortKeyEnd)));

    // Verify query response
    ItemResponse queryResponse = responses.get(queryId.toString());
    assertNotNull(queryResponse, "Expected query response");
    assertTrue(queryResponse.hasQueryResponse(), "Expected query result");

    ItemResponse.QueryResponse queryResult = queryResponse.getQueryResponse();
    assertEquals(
        numberOfQueryItems,
        queryResult.getItemsQueryResult().getItemsCount(),
        "Expected to receive " + numberOfQueryItems + " inserted items");

    // Verify query results contain all inserted items
    Set<String> returnedSortKeys =
        queryResult.getItemsQueryResult().getItemsList().stream()
            .map(item -> item.getKey().getSortKey())
            .collect(Collectors.toSet());

    for (int i = 0; i < numberOfQueryItems; i++) {
      String expectedSortKey = sortKeyPrefix + i;
      assertTrue(
          returnedSortKeys.contains(expectedSortKey),
          "Expected to find item with sort key: " + expectedSortKey);
    }
  }

  @Test
  void handleRockDBExceptions() throws RocksDBException, InterruptedException {
    UUID putItemId = UUID.randomUUID();
    String table = "table";
    String partitionKey = "pk1";
    String sortKey = "sk1";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("field1", "value1");
    attributes.put("field2", 123.0);
    attributes.put("field3", true);

    CountDownLatch latch = new CountDownLatch(1);
    Map<String, ItemResponse> responses = new HashMap<>();

    // mock throw exception
    doThrow(new RocksDBException("Test exception"))
        .when(roxDB)
        .putItem(eq(table), any(com.github.lukaszbudnik.roxdb.rocksdb.Item.class));

    ItemRequest putItemRequest =
        ItemRequest.newBuilder()
            .setCorrelationId(putItemId.toString())
            .setPutItem(
                ItemRequest.PutItem.newBuilder()
                    .setTable(table)
                    .setItem(
                        Item.newBuilder()
                            .setKey(
                                Key.newBuilder()
                                    .setPartitionKey(partitionKey)
                                    .setSortKey(sortKey)
                                    .build())
                            .setAttributes(ProtoUtils.mapToStruct(attributes))
                            .build())
                    .build())
            .build();

    var responseObserver =
        new StreamObserver<ItemResponse>() {
          @Override
          public void onNext(ItemResponse itemResponse) {
            responses.put(itemResponse.getCorrelationId(), itemResponse);
            latch.countDown();
          }

          @Override
          public void onError(Throwable throwable) {
            fail("onError should not be called");
          }

          @Override
          public void onCompleted() {
            // no-op
          }
        };

    StreamObserver<ItemRequest> requestObserver = asyncStub.processItems(responseObserver);
    requestObserver.onNext(putItemRequest);
    requestObserver.onCompleted();

    assertTrue(latch.await(1, TimeUnit.SECONDS));

    // verify Mocks were called
    verify(roxDB).putItem(eq(table), itemCaptor.capture());
    var capturedItem = itemCaptor.getValue();
    assertEquals(partitionKey, capturedItem.key().partitionKey());
    assertEquals(sortKey, capturedItem.key().sortKey());
    assertEquals(attributes, capturedItem.attributes());

    // verify expected gRPC response
    ItemResponse putItemResponse = responses.get(putItemId.toString());
    assertTrue(putItemResponse.hasError(), "Expected to get error response for " + putItemId);
    assertEquals(putItemId.toString(), putItemResponse.getCorrelationId());
    assertEquals(Error.ROCKS_DB_ERROR.getCode(), putItemResponse.getError().getCode());
    assertEquals(Error.ROCKS_DB_ERROR.getMessage(), putItemResponse.getError().getMessage());
  }
}
