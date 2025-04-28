package com.github.lukaszbudnik.roxdb.grpc;

import static com.github.lukaszbudnik.roxdb.rocksdb.RoxDBImpl.PARTITION_SORT_KEY_SEPARATOR;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.github.lukaszbudnik.roxdb.rocksdb.*;
import com.github.lukaszbudnik.roxdb.v1.*;
import com.github.lukaszbudnik.roxdb.v1.Item;
import com.github.lukaszbudnik.roxdb.v1.Key;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.grpc.*;
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
  void query() throws RocksDBException, InterruptedException {
    // Setup test data
    String table = "table";
    String partitionKey = "test-partition";
    String sortKeyPrefix = "sort-key-";
    int numberOfQueryItems = 3;

    Map<String, ItemResponse> responses = new HashMap<>();
    CountDownLatch latch = new CountDownLatch(1);

    // setup RoxDB.query mock to return 3 items
    Optional<SortKeyRange> sortKeyRange =
        Optional.of(
            SortKeyRange.between(
                RangeBoundary.inclusive(sortKeyPrefix + "0"),
                RangeBoundary.inclusive(sortKeyPrefix + "2")));

    when(roxDB.query(eq(table), eq(partitionKey), eq(10), eq(sortKeyRange)))
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
            .setLimit(10)
            .setSortKeyRange(
                ItemRequest.SortKeyRange.newBuilder()
                    .setStart(
                        ItemRequest.RangeBoundary.newBuilder()
                            .setType(ItemRequest.RangeType.INCLUSIVE)
                            .setValue(sortKeyStart)
                            .build())
                    .setEnd(
                        ItemRequest.RangeBoundary.newBuilder()
                            .setType(ItemRequest.RangeType.INCLUSIVE)
                            .setValue(sortKeyEnd)
                            .build())
                    .build())
            .build();

    ItemRequest request =
        ItemRequest.newBuilder()
            .setCorrelationId(queryId.toString())
            .setQuery(queryRequest)
            .build();

    requestObserver.onNext(request);
    requestObserver.onCompleted();

    assertTrue(latch.await(1, TimeUnit.SECONDS));

    verify(roxDB).query(eq(table), eq(partitionKey), eq(10), eq(sortKeyRange));

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
    Map<Status.Code, StatusRuntimeException> responses = new HashMap<>();

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
            fail("onNext should not be called");
          }

          @Override
          public void onError(Throwable throwable) {
            StatusRuntimeException exception = (StatusRuntimeException) throwable;
            responses.put(exception.getStatus().getCode(), exception);
            latch.countDown();
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
    StatusRuntimeException putItemException = responses.get(Status.INTERNAL.getCode());
    assertNotNull(putItemException, "Expected to get put item exception");
    assertEquals("Internal server error", putItemException.getStatus().getDescription());
    // verify Metadata correlationId
    assertEquals(
        putItemId.toString(),
        putItemException
            .getTrailers()
            .get(Metadata.Key.of("correlationId", Metadata.ASCII_STRING_MARSHALLER)));
    // verify Metadata exception
    assertEquals(
        RocksDBException.class.getCanonicalName(),
        putItemException
            .getTrailers()
            .get(Metadata.Key.of("exception", Metadata.ASCII_STRING_MARSHALLER)));
  }

  @Test
  void transactWriteItems() throws InterruptedException, RocksDBException {
    // Create test data
    UUID transactWriteItemsId = UUID.randomUUID();

    // Create a Key
    Key key1 = Key.newBuilder().setPartitionKey("customer#122").setSortKey("order#456").build();

    // Create attributes for the item
    Struct.Builder attributes1 = Struct.newBuilder();
    attributes1.putFields("name", Value.newBuilder().setStringValue("Test Item").build());
    attributes1.putFields("price", Value.newBuilder().setNumberValue(99.99).build());

    // Create an Item
    Item item1 = Item.newBuilder().setKey(key1).setAttributes(attributes1.build()).build();

    // Create PutItem operation
    String table1 = "table1";
    ItemRequest.PutItem putItem =
        ItemRequest.PutItem.newBuilder().setTable(table1).setItem(item1).build();

    Key key2 = Key.newBuilder().setPartitionKey("customer#123").setSortKey("order#456").build();

    // Create attributes for the item
    Struct.Builder attributes2 = Struct.newBuilder();
    attributes2.putFields("name", Value.newBuilder().setStringValue("Test Item").build());
    attributes2.putFields("price", Value.newBuilder().setNumberValue(99.99).build());

    // Create an Item
    Item item2 = Item.newBuilder().setKey(key2).setAttributes(attributes2.build()).build();
    // Update an Item
    String table2 = "table2";
    ItemRequest.UpdateItem updateItem =
        ItemRequest.UpdateItem.newBuilder().setTable(table2).setItem(item2).build();

    // Create DeleteItem operation
    Key key3 = Key.newBuilder().setPartitionKey("customer#124").setSortKey("order#457").build();
    String table3 = "table3";
    ItemRequest.DeleteItem deleteItem =
        ItemRequest.DeleteItem.newBuilder().setTable(table3).setKey(key3).build();

    // Create TransactWriteItems with multiple operations
    ItemRequest.TransactWriteItems transactWriteItems =
        ItemRequest.TransactWriteItems.newBuilder()
            .addItems(ItemRequest.TransactWriteItem.newBuilder().setPut(putItem).build())
            .addItems(ItemRequest.TransactWriteItem.newBuilder().setUpdate(updateItem).build())
            .addItems(ItemRequest.TransactWriteItem.newBuilder().setDelete(deleteItem).build())
            .build();

    // Create the main ItemRequest
    ItemRequest request =
        ItemRequest.newBuilder()
            .setCorrelationId(transactWriteItemsId.toString())
            .setTransactWriteItems(transactWriteItems)
            .build();

    CountDownLatch latch = new CountDownLatch(1);
    Map<String, ItemResponse> responses = new HashMap<>();

    TransactionContext mockedTxContext = mock(TransactionContext.class);

    doAnswer(
            invocation -> {
              // Get the TransactionOperations (the lambda) passed as the argument
              TransactionOperations transactionOperationsCallback = invocation.getArgument(0);

              // Execute the lambda, passing the mocked TransactionContext
              transactionOperationsCallback.doInTransaction(mockedTxContext);

              // Simulate successful execution by returning null (since the interface is Void)
              return null;
            })
        .when(roxDB)
        .executeTransaction(any(TransactionOperations.class));

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
    requestObserver.onNext(request);
    requestObserver.onCompleted();

    assertTrue(latch.await(1, TimeUnit.SECONDS));

    // verify Mocks were called
    verify(roxDB, times(1)).executeTransaction(any(TransactionOperations.class));

    // Verify that the correct operations were called on the mocked TransactionContext
    verify(mockedTxContext, times(1))
        .put(eq(table1), any(com.github.lukaszbudnik.roxdb.rocksdb.Item.class));
    verify(mockedTxContext, times(1))
        .update(eq(table2), any(com.github.lukaszbudnik.roxdb.rocksdb.Item.class));
    verify(mockedTxContext, times(1))
        .delete(eq(table3), any(com.github.lukaszbudnik.roxdb.rocksdb.Key.class));

    // verify expected gRPC response
    ItemResponse transactWriteItemsResponse = responses.get(transactWriteItemsId.toString());
    assertTrue(
        transactWriteItemsResponse.hasTransactWriteItemsResponse(),
        "Expected to get transact write items response for " + transactWriteItemsId);
    assertEquals(transactWriteItemsId.toString(), transactWriteItemsResponse.getCorrelationId());
    assertEquals(
        3,
        transactWriteItemsResponse
            .getTransactWriteItemsResponse()
            .getTransactWriteItemsResult()
            .getModifiedKeysCount(),
        "Expected to receive 2 items in transact write items response");
    // first key in response should be the one from put item
    assertEquals(
        key1.getPartitionKey() + "#" + key1.getSortKey(),
        transactWriteItemsResponse
                .getTransactWriteItemsResponse()
                .getTransactWriteItemsResult()
                .getModifiedKeys(0)
                .getPartitionKey()
            + "#"
            + transactWriteItemsResponse
                .getTransactWriteItemsResponse()
                .getTransactWriteItemsResult()
                .getModifiedKeys(0)
                .getSortKey());
    // second key in response should be the one from update item
    assertEquals(
        key2.getPartitionKey() + "#" + key2.getSortKey(),
        transactWriteItemsResponse
                .getTransactWriteItemsResponse()
                .getTransactWriteItemsResult()
                .getModifiedKeys(1)
                .getPartitionKey()
            + "#"
            + transactWriteItemsResponse
                .getTransactWriteItemsResponse()
                .getTransactWriteItemsResult()
                .getModifiedKeys(1)
                .getSortKey());
    // third key in response should be the one from delete item
    assertEquals(
        key3.getPartitionKey() + "#" + key3.getSortKey(),
        transactWriteItemsResponse
                .getTransactWriteItemsResponse()
                .getTransactWriteItemsResult()
                .getModifiedKeys(2)
                .getPartitionKey()
            + "#"
            + transactWriteItemsResponse
                .getTransactWriteItemsResponse()
                .getTransactWriteItemsResult()
                .getModifiedKeys(2)
                .getSortKey());
  }

  @Test
  void putItemValidationErrors() throws RocksDBException, InterruptedException {
    UUID putItemId = UUID.randomUUID();
    String table = "table";
    String partitionKey = "";
    String sortKey = "";
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("field1", "value1");
    attributes.put("field2", 123.0);
    attributes.put("field3", true);

    CountDownLatch latch = new CountDownLatch(1);
    Map<String, ItemResponse> responses = new HashMap<>();

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

    // verify gRPC response contains Errors message
    ItemResponse putItemResponse = responses.get(putItemId.toString());
    assertTrue(putItemResponse.hasErrors(), "Expected to get errors for " + putItemId);
    assertEquals(putItemId.toString(), putItemResponse.getCorrelationId());
    assertEquals(
        2,
        putItemResponse.getErrors().getErrorCount(),
        "Expected to get 2 errors for " + putItemId);
    assertEquals(
        "Partition key cannot be blank", putItemResponse.getErrors().getError(0).getMessage());
    assertEquals("Sort key cannot be blank", putItemResponse.getErrors().getError(1).getMessage());
  }

  @Test
  void transactWriteItemsWithValidationErrors() throws InterruptedException {
    // Create test data
    UUID transactWriteItemsId = UUID.randomUUID();

    // Create a Key
    Key key1 =
        Key.newBuilder().setPartitionKey("" + PARTITION_SORT_KEY_SEPARATOR).setSortKey("").build();

    // Create attributes for the item
    Struct.Builder attributes1 = Struct.newBuilder();
    attributes1.putFields("name", Value.newBuilder().setStringValue("Test Item").build());
    attributes1.putFields("price", Value.newBuilder().setNumberValue(99.99).build());

    // Create an Item
    Item item1 = Item.newBuilder().setKey(key1).setAttributes(attributes1.build()).build();

    // Create PutItem operation
    String table1 = "table1";
    ItemRequest.PutItem putItem =
        ItemRequest.PutItem.newBuilder().setTable(table1).setItem(item1).build();

    Key key2 =
        Key.newBuilder().setPartitionKey("").setSortKey("" + PARTITION_SORT_KEY_SEPARATOR).build();

    // Create attributes for the item
    Struct.Builder attributes2 = Struct.newBuilder();
    attributes2.putFields("name", Value.newBuilder().setStringValue("Test Item").build());
    attributes2.putFields("price", Value.newBuilder().setNumberValue(99.99).build());

    // Create an Item
    Item item2 = Item.newBuilder().setKey(key2).setAttributes(attributes2.build()).build();
    // Update an Item
    String table2 = "table2";
    ItemRequest.UpdateItem updateItem =
        ItemRequest.UpdateItem.newBuilder().setTable(table2).setItem(item2).build();

    // Create DeleteItem operation
    Key key3 =
        Key.newBuilder().setPartitionKey("" + PARTITION_SORT_KEY_SEPARATOR).setSortKey("").build();
    String table3 = "table3";
    ItemRequest.DeleteItem deleteItem =
        ItemRequest.DeleteItem.newBuilder().setTable(table3).setKey(key3).build();

    // Create TransactWriteItems with multiple operations
    ItemRequest.TransactWriteItems transactWriteItems =
        ItemRequest.TransactWriteItems.newBuilder()
            .addItems(ItemRequest.TransactWriteItem.newBuilder().setPut(putItem).build())
            .addItems(ItemRequest.TransactWriteItem.newBuilder().setUpdate(updateItem).build())
            .addItems(ItemRequest.TransactWriteItem.newBuilder().setDelete(deleteItem).build())
            .build();

    // Create the main ItemRequest
    ItemRequest request =
        ItemRequest.newBuilder()
            .setCorrelationId(transactWriteItemsId.toString())
            .setTransactWriteItems(transactWriteItems)
            .build();

    CountDownLatch latch = new CountDownLatch(1);
    Map<String, ItemResponse> responses = new HashMap<>();

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
    requestObserver.onNext(request);
    requestObserver.onCompleted();

    assertTrue(latch.await(1, TimeUnit.SECONDS));
    // verify gRPC response contains Errors message
    ItemResponse transactWriteItemsResponse = responses.get(transactWriteItemsId.toString());
    assertTrue(
        transactWriteItemsResponse.hasErrors(),
        "Expected to get errors for " + transactWriteItemsId);
    assertEquals(transactWriteItemsId.toString(), transactWriteItemsResponse.getCorrelationId());
    assertEquals(
        6,
        transactWriteItemsResponse.getErrors().getErrorCount(),
        "Expected to get 6 errors for " + transactWriteItemsId);
    assertEquals(
        "Partition key cannot contain character U+001F",
        transactWriteItemsResponse.getErrors().getError(0).getMessage());
    assertEquals(
        "Sort key cannot be blank",
        transactWriteItemsResponse.getErrors().getError(1).getMessage());
    assertEquals(
        "Partition key cannot be blank",
        transactWriteItemsResponse.getErrors().getError(2).getMessage());
    assertEquals(
        "Sort key cannot contain character U+001F",
        transactWriteItemsResponse.getErrors().getError(3).getMessage());
    assertEquals(
        "Partition key cannot contain character U+001F",
        transactWriteItemsResponse.getErrors().getError(4).getMessage());
    assertEquals(
        "Sort key cannot be blank",
        transactWriteItemsResponse.getErrors().getError(5).getMessage());
  }

  @Test
  void queryWithValidationErrors() throws RocksDBException, InterruptedException {
    UUID queryId = UUID.randomUUID();
    String table = "table";
    String partitionKey = "";
    String sortKey = "";

    CountDownLatch latch = new CountDownLatch(1);
    Map<String, ItemResponse> responses = new HashMap<>();

    ItemRequest queryRequest =
        ItemRequest.newBuilder()
            .setCorrelationId(queryId.toString())
            .setQuery(
                ItemRequest.Query.newBuilder()
                    .setTable(table)
                    .setPartitionKey(partitionKey)
                    .setLimit(10)
                    .setSortKeyRange(
                        ItemRequest.SortKeyRange.newBuilder()
                            .setStart(
                                ItemRequest.RangeBoundary.newBuilder()
                                    .setType(ItemRequest.RangeType.INCLUSIVE)
                                    .setValue(sortKey)
                                    .build())
                            .setEnd(
                                ItemRequest.RangeBoundary.newBuilder()
                                    .setType(ItemRequest.RangeType.INCLUSIVE)
                                    .setValue(sortKey + PARTITION_SORT_KEY_SEPARATOR)
                                    .build())
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
    requestObserver.onNext(queryRequest);
    requestObserver.onCompleted();

    assertTrue(latch.await(1, TimeUnit.SECONDS));

    // verify gRPC response contains Errors message
    ItemResponse queryResponse = responses.get(queryId.toString());
    assertTrue(queryResponse.hasErrors(), "Expected to get errors for " + queryId);
    assertEquals(queryId.toString(), queryResponse.getCorrelationId());
    assertEquals(3, queryResponse.getErrors().getErrorCount());
    assertEquals(
        "Partition key cannot be blank", queryResponse.getErrors().getError(0).getMessage());
    assertEquals("Sort key cannot be blank", queryResponse.getErrors().getError(1).getMessage());
    assertEquals(
        "Sort key cannot contain character U+001F",
        queryResponse.getErrors().getError(2).getMessage());
  }

  @Test
  void queryWithSortKeyRangeEmpty() throws RocksDBException, InterruptedException {
    UUID queryId = UUID.randomUUID();
    String table = "table";
    String partitionKey = "";

    CountDownLatch latch = new CountDownLatch(1);
    Map<String, ItemResponse> responses = new HashMap<>();

    ItemRequest queryRequest =
        ItemRequest.newBuilder()
            .setCorrelationId(queryId.toString())
            .setQuery(
                ItemRequest.Query.newBuilder()
                    .setTable(table)
                    .setPartitionKey(partitionKey)
                    .setLimit(10)
                    .setSortKeyRange(ItemRequest.SortKeyRange.newBuilder().build())
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
    requestObserver.onNext(queryRequest);
    requestObserver.onCompleted();

    assertTrue(latch.await(1, TimeUnit.SECONDS));

    // verify gRPC response contains Errors message
    ItemResponse queryResponse = responses.get(queryId.toString());
    assertTrue(queryResponse.hasErrors(), "Expected to get errors for " + queryId);
    assertEquals(queryId.toString(), queryResponse.getCorrelationId());
    assertEquals(1, queryResponse.getErrors().getErrorCount());
    assertEquals(
        "When set SortKeyRange must have at least one boundary",
        queryResponse.getErrors().getError(0).getMessage());
  }
}
