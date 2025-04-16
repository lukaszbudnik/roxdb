package com.github.lukaszbudnik.roxdb.grpc;

import com.github.lukaszbudnik.roxdb.v1.RoxDBGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class HealthServiceTest {

    private Server server;
    private ManagedChannel channel;
    private HealthService healthService;

    @BeforeEach
    void setUp() throws IOException, RocksDBException {
        String serverName = InProcessServerBuilder.generateName();

        healthService = new HealthService();

        server = InProcessServerBuilder.forName(serverName).directExecutor().addService(healthService).build().start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    void check() {
        HealthGrpc.HealthBlockingStub healthStub = HealthGrpc.newBlockingStub(channel);

        // Set the status
        healthService.setServiceStatus(RoxDBGrpc.getServiceDescriptor().getName(), HealthCheckResponse.ServingStatus.SERVING);

        // Build and send the health check request
        HealthCheckRequest healthCheckRequest = HealthCheckRequest.newBuilder().setService(RoxDBGrpc.getServiceDescriptor().getName()).build();
        HealthCheckResponse healthCheckResponse = healthStub.check(healthCheckRequest);

        assertEquals(HealthCheckResponse.ServingStatus.SERVING, healthCheckResponse.getStatus());
    }
}