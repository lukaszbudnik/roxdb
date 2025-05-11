package com.github.lukaszbudnik.roxdb.metrics;

import java.util.List;

public record MetricsConfig(
    String otlpEndpoint, int interval, List<String> tickers, List<String> histograms) {}
