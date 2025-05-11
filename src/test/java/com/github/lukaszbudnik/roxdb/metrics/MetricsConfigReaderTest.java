package com.github.lukaszbudnik.roxdb.metrics;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MetricsConfigReaderTest {

  @TempDir Path tempDir;

  @Test
  void readConfig() throws IOException {
    // Create test yaml file
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

    Files.write(metricsConfig, metricsConfigContent.getBytes());

    MetricsConfigReader reader = new MetricsConfigReader();
    MetricsConfig config = reader.readConfig(metricsConfig.toString());
    assertNotNull(config);
    assertEquals("http://localhost:4317", config.otlpEndpoint());
    assertEquals(2, config.tickers().size());
    assertEquals("AAA", config.tickers().get(0));
    assertEquals("BBB", config.tickers().get(1));
    assertEquals(1, config.histograms().size());
    assertEquals("*", config.histograms().get(0));
  }
}
