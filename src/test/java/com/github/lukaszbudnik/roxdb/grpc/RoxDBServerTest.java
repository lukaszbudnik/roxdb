package com.github.lukaszbudnik.roxdb.grpc;

import com.github.lukaszbudnik.roxdb.db.RoxDBImpl;
import com.github.lukaszbudnik.roxdb.v1.*;
import io.grpc.stub.StreamObserver;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class RoxDBServerTest {
    private static final int port = 50052;
    private static RoxDBServer server;
    private static RoxDBImpl roxDB;
    private static String dbPath;
    private static io.grpc.Channel channel;
    private static RoxDBGrpc.RoxDBStub asyncStub;

    @BeforeAll
    static void setUp() throws Exception {
        // Create a unique test database path
        dbPath = "/tmp/test-db-" + UUID.randomUUID();
        // Initialize RoxDB
        roxDB = new RoxDBImpl(dbPath);
        // Create the gRPC server
        server = new RoxDBServer(port, roxDB);
        server.start();
        // create channel to server
        channel = io.grpc.ManagedChannelBuilder.forAddress("localhost", port)
                                               .usePlaintext()
                                               .build();
        // Initialize the async stub
        asyncStub = RoxDBGrpc.newStub(channel);
    }

    @AfterAll
    static void tearDown() throws Exception {
        server.stop();
        roxDB.close();
        FileUtils.deleteDirectory(new File(dbPath));
    }

    @Test
    void testServer() throws Exception {
        // Prepare test data
        String partitionKey = "test-key";
        String sortKey = "test-value";
        String tableName = "test";
        Key key = Key.newBuilder().setPartitionKey(partitionKey).setSortKey(sortKey).build();
        UUID putItemId = UUID.randomUUID();
        Map<String, ItemResponse> responses = new HashMap<>();

        ItemRequest putRequest = ItemRequest.newBuilder()
                                            .setCorrelationId(putItemId.toString())
                                            .setPutItem(ItemRequest.PutItem.newBuilder().setTable(tableName).setItem(Item.newBuilder().setKey(key).build()).build())
                                            .build()
                ;

        // connect to gRPC server and send putRequest
        CountDownLatch latch = new CountDownLatch(1);
        StreamObserver<ItemRequest> putItemRequestStreamObserver = asyncStub.processItems(new StreamObserver<ItemResponse>() {
            @Override
            public void onNext(ItemResponse response) {
                responses.put(response.getCorrelationId(), response);
                latch.countDown();
            }

            @Override
            public void onError(Throwable throwable) {
                fail("Error occurred: " + throwable.getMessage());
            }

            @Override
            public void onCompleted() {
                // No action needed
            }
        });

        putItemRequestStreamObserver.onNext(putRequest);
        putItemRequestStreamObserver.onCompleted();
        boolean await = latch.await(5, TimeUnit.SECONDS);
        assertTrue(await, "Timeout waiting for response");
        ItemResponse response = responses.get(putItemId.toString());
        assertTrue(response.hasPutItemResponse(), "Response should have a putItemResponse");
        assertEquals(putItemId.toString(), response.getCorrelationId());
    }

}
