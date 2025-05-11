package com.github.lukaszbudnik.roxdb.metrics;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import java.time.Duration;

public class OtlpMetricsExporterFactory implements MetricsExporterFactory {
  private final MetricsConfig config;
  private MetricExporter metricExporter;

  public OtlpMetricsExporterFactory(MetricsConfig config) {
    this.config = config;
  }

  @Override
  public MetricExporter createMetricExporter() {
    if (metricExporter == null) {
      metricExporter = OtlpGrpcMetricExporter.builder().setEndpoint(config.otlpEndpoint()).build();
    }
    return metricExporter;
  }

  @Override
  public Meter createMeter() {
    MetricExporter metricExporter = createMetricExporter();

    SdkMeterProvider meterProvider =
        SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(metricExporter)
                    .setInterval(Duration.ofSeconds(config.interval()))
                    .build())
            .build();

    OpenTelemetrySdk openTelemetry =
        OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build();

    Meter meter = openTelemetry.getMeter("rocksdb-metrics");
    return meter;
  }
}
