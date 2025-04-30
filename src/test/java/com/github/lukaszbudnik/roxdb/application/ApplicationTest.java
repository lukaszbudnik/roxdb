package com.github.lukaszbudnik.roxdb.application;

import static com.github.lukaszbudnik.roxdb.application.EnvironmentConfigReader.*;
import static org.junit.jupiter.api.Assertions.*;

import io.grpc.health.v1.HealthCheckResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

class ApplicationTest {
  @Test
  void shouldInitializeApplication() throws IOException, RocksDBException, InterruptedException {

    // set up environment variables
    Map<String, String> env = new HashMap<>();
    env.put(ENV_PORT, String.valueOf(DEFAULT_PORT + 1));

    Application application = new Application(env);
    application.startApplication();

    // then
    RoxDBConfig config = application.getConfig();
    assertEquals(DEFAULT_DB_PATH, config.dbPath());
    assertEquals(DEFAULT_PORT + 1, config.port());
    assertEquals(
        HealthCheckResponse.ServingStatus.SERVING, application.getServer().getServiceStatus());

    application.getServer().stop();

    assertEquals(
        HealthCheckResponse.ServingStatus.NOT_SERVING, application.getServer().getServiceStatus());
  }
}
