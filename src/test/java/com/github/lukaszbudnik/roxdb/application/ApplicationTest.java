package com.github.lukaszbudnik.roxdb.application;

import static com.github.lukaszbudnik.roxdb.application.EnvironmentConfigReader.*;
import static org.junit.jupiter.api.Assertions.*;

import io.grpc.health.v1.HealthCheckResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;

class ApplicationTest {

  @TempDir Path tempDir;

  @Test
  void shouldInitializeApplication() throws IOException, RocksDBException, InterruptedException {

    Path metricsConfig = tempDir.resolve("metrics.yaml");
    String metricsConfigContent =
        """
            otlpEndpoint: http://localhost:4317
            interval: 10
            tickers:
             - "AAA"
             - "BBB"
            histograms:
             - "*"
            """;

    Path dbPath = tempDir.resolve("rocksdb");

    Files.write(metricsConfig, metricsConfigContent.getBytes());

    // set up environment variables
    Map<String, String> env = new HashMap<>();
    env.put(ENV_PORT, String.valueOf(DEFAULT_PORT + 1));
    env.put(ENV_OPENTELEMETRY_CONFIG, metricsConfig.toString());
    env.put(ENV_DB_PATH, dbPath.toString());

    Application application = new Application(env);
    application.startApplication();

    // then
    RoxDBConfig config = application.getConfig();
    assertEquals(dbPath.toString(), config.dbPath());
    assertEquals(DEFAULT_PORT + 1, config.port());
    assertEquals(
        HealthCheckResponse.ServingStatus.SERVING, application.getServer().getServiceStatus());

    application.getServer().stop();

    assertEquals(
        HealthCheckResponse.ServingStatus.NOT_SERVING, application.getServer().getServiceStatus());
  }
}
