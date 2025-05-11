package com.github.lukaszbudnik.roxdb.metrics;

import io.opentelemetry.api.metrics.Meter;
import java.util.List;
import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;

public class RocksDBMetricsCollector {
  private final Statistics statistics;
  private final Meter meter;

  public RocksDBMetricsCollector(Statistics statistics, Meter meter) {
    this.statistics = statistics;
    this.meter = meter;
  }

  public void createTickerTypeMetrics(List<TickerType> tickerTypes) {
    tickerTypes.forEach(this::createGauge);
  }

  private void createGauge(TickerType tickerType) {
    String name = tickerType.name().toLowerCase();
    meter
        .gaugeBuilder(name)
        .buildWithCallback(
            measurement -> measurement.record(statistics.getTickerCount(tickerType)));
  }

  public void createHistogramTypeMetrics(List<HistogramType> histogramTypes) {
    histogramTypes.forEach(this::createGaugeFromHistogramData);
  }

  private void createGaugeFromHistogramData(HistogramType type) {
    String name = type.name().toLowerCase();
    meter
        .gaugeBuilder(name + "_median")
        .buildWithCallback(
            measurement -> {
              HistogramData data = statistics.getHistogramData(type);
              measurement.record(data.getMedian());
            });
    meter
        .gaugeBuilder(name + "_p95")
        .buildWithCallback(
            measurement -> {
              HistogramData data = statistics.getHistogramData(type);
              measurement.record(data.getPercentile95());
            });
    meter
        .gaugeBuilder(name + "_p99")
        .buildWithCallback(
            measurement -> {
              HistogramData data = statistics.getHistogramData(type);
              measurement.record(data.getPercentile99());
            });
    meter
        .gaugeBuilder(name + "_average")
        .buildWithCallback(
            measurement -> {
              HistogramData data = statistics.getHistogramData(type);
              measurement.record(data.getAverage());
            });
    meter
        .gaugeBuilder(name + "_std_dev")
        .buildWithCallback(
            measurement -> {
              HistogramData data = statistics.getHistogramData(type);
              measurement.record(data.getStandardDeviation());
            });
    meter
        .gaugeBuilder(name + "_max")
        .buildWithCallback(
            measurement -> {
              HistogramData data = statistics.getHistogramData(type);
              measurement.record(data.getMax());
            });
    meter
        .gaugeBuilder(name + "_min")
        .buildWithCallback(
            measurement -> {
              HistogramData data = statistics.getHistogramData(type);
              measurement.record(data.getMin());
            });
    meter
        .gaugeBuilder(name + "_count")
        .buildWithCallback(
            measurement -> {
              HistogramData data = statistics.getHistogramData(type);
              measurement.record(data.getCount());
            });
    meter
        .gaugeBuilder(name + "_sum")
        .buildWithCallback(
            measurement -> {
              HistogramData data = statistics.getHistogramData(type);
              measurement.record(data.getSum());
            });
  }
}
