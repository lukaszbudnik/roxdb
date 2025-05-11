package com.github.lukaszbudnik.roxdb.metrics;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.rocksdb.HistogramType;
import org.rocksdb.TickerType;

public class MetricsConfigProcessor {
  private final MetricsConfig config;

  public MetricsConfigProcessor(MetricsConfig config) {
    this.config = config;
  }

  public List<TickerType> getTickerTypes() {
    return config.tickers().contains("*")
        ? Arrays.asList(TickerType.values())
        : config.tickers().stream()
            .map(this::parseTickerType)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
  }

  private TickerType parseTickerType(String ticker) {
    try {
      return TickerType.valueOf(ticker.toUpperCase());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  public List<HistogramType> getHistogramTypes() {
    return config.histograms().contains("*")
        ? Arrays.asList(HistogramType.values())
        : config.histograms().stream()
            .map(this::parseHistogramType)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
  }

  private HistogramType parseHistogramType(String histogram) {
    try {
      return HistogramType.valueOf(histogram.toUpperCase());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
