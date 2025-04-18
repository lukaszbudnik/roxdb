package com.github.lukaszbudnik.roxdb.grpc;

import static org.junit.jupiter.api.Assertions.*;

import com.github.lukaszbudnik.roxdb.rocksdb.RoxDB;
import io.grpc.health.v1.HealthCheckResponse;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

class RoxDBServerTest {
  private final int port = 50052;
  @Mock private RoxDB roxDB;

  @Test
  void testServer() throws Exception {
    RoxDBServer server = new RoxDBServer(port, roxDB);
    server.start();
    HealthCheckResponse.ServingStatus serviceStatusBefore = server.getServiceStatus();
    assertEquals(HealthCheckResponse.ServingStatus.SERVING, serviceStatusBefore);
    server.stop();
    HealthCheckResponse.ServingStatus serviceStatusAfter = server.getServiceStatus();
    assertEquals(HealthCheckResponse.ServingStatus.NOT_SERVING, serviceStatusAfter);
  }
}
