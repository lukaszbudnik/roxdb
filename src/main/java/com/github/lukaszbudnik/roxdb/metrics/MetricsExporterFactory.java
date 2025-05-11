package com.github.lukaszbudnik.roxdb.metrics;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.export.MetricExporter;

public interface MetricsExporterFactory {
  MetricExporter createMetricExporter();

  Meter createMeter();
}
