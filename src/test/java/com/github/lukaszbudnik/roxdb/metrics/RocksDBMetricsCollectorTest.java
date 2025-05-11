package com.github.lukaszbudnik.roxdb.metrics;

import static org.mockito.Mockito.*;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricExporter;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.rocksdb.HistogramData;
import org.rocksdb.Statistics;

@ExtendWith(MockitoExtension.class)
class RocksDBMetricsCollectorTest {
  @Mock Statistics statistics;
  Meter meter;
  MetricsConfig metricsConfig;

  @BeforeEach
  void setup() {
    metricsConfig = new MetricsConfig("", 1, List.of("BLOCK_CACHE_HIT"), List.of("DB_GET"));
    MetricsExporterFactory metricsExporterFactory =
        new OtlpMetricsExporterFactory(metricsConfig) {
          @Override
          public MetricExporter createMetricExporter() {
            return InMemoryMetricExporter.create();
          }
        };
    meter = metricsExporterFactory.createMeter();
  }

  @Test
  void testTickerType() throws InterruptedException {
    MetricsConfigProcessor metricsConfigProcessor = new MetricsConfigProcessor(metricsConfig);

    when(statistics.getTickerCount(eq(metricsConfigProcessor.getTickerTypes().getFirst())))
        .thenReturn(1000L);

    RocksDBMetricsCollector collector = new RocksDBMetricsCollector(statistics, meter);
    collector.createTickerTypeMetrics(metricsConfigProcessor.getTickerTypes());

    // metrics are read every 1s, sleep for 1.1s
    Thread.sleep(Duration.ofSeconds(1).plusMillis(100));

    verify(statistics).getTickerCount(metricsConfigProcessor.getTickerTypes().getFirst());
  }

  @Test
  void testHistogramType() throws InterruptedException {
    MetricsConfigProcessor metricsConfigProcessor = new MetricsConfigProcessor(metricsConfig);

    when(statistics.getHistogramData(eq(metricsConfigProcessor.getHistogramTypes().getFirst())))
        .thenReturn(new HistogramData(0d, 0d, 0d, 0d, 0d));

    RocksDBMetricsCollector collector = new RocksDBMetricsCollector(statistics, meter);
    collector.createHistogramTypeMetrics(metricsConfigProcessor.getHistogramTypes());

    // metrics are read every 1s, sleep fo 1.1s
    Thread.sleep(Duration.ofSeconds(1).plusMillis(100));

    String[] gauges =
        new String[] {
          "_median", "_p95", "_p99", "_average", "_std_dev", "_max", "_min", "_count", "_sum"
        };

    verify(statistics, times(gauges.length))
        .getHistogramData(metricsConfigProcessor.getHistogramTypes().getFirst());
  }
}
