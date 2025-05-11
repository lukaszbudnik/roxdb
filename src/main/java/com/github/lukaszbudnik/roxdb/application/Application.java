package com.github.lukaszbudnik.roxdb.application;

import com.github.lukaszbudnik.roxdb.grpc.RoxDBGrpcService;
import com.github.lukaszbudnik.roxdb.grpc.RoxDBServer;
import com.github.lukaszbudnik.roxdb.metrics.*;
import com.github.lukaszbudnik.roxdb.rocksdb.RoxDB;
import com.github.lukaszbudnik.roxdb.rocksdb.RoxDBImpl;
import com.google.common.base.Strings;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.io.IOException;
import java.util.Map;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {
  private static final Logger logger = LoggerFactory.getLogger(Application.class);

  private final RoxDBConfig config;
  private RoxDBServer server;

  public Application(Map<String, String> env) {
    EnvironmentConfigReader configReader = new EnvironmentConfigReader();
    config = configReader.readConfiguration(env);

    ConfigurationValidator configurationValidator = new ConfigurationValidator();
    configurationValidator.validateConfiguration(config);
  }

  public static void main(String[] args) {
    try {
      logger.info("Starting RoxDB");
      Application application = new Application(System.getenv());
      application.startApplication();
      application.blockUntilShutdown();
    } catch (Throwable t) {
      logger.error("Unexpected error", t);
      System.exit(1);
    }
  }

  void startApplication() throws RocksDBException, IOException {
    RoxDB roxDB = new RoxDBImpl(config.dbPath());
    this.server = new RoxDBServer(config, new RoxDBGrpcService(roxDB));
    MetricExporter metricExporter = null;
    if (!Strings.isNullOrEmpty(config.openTelemetryConfig())) {
      MetricsConfigReader metricsConfigReader = new MetricsConfigReader();
      MetricsConfig metricsConfig = metricsConfigReader.readConfig(config.openTelemetryConfig());
      OtlpMetricsExporterFactory metricsExporterFactory =
          new OtlpMetricsExporterFactory(metricsConfig);
      metricExporter = metricsExporterFactory.createMetricExporter();
      Meter meter = metricsExporterFactory.createMeter();
      RocksDBMetricsCollector metricsCollector =
          new RocksDBMetricsCollector(roxDB.getStatistics(), meter);
      MetricsConfigProcessor metricsConfigProcessor = new MetricsConfigProcessor(metricsConfig);
      metricsCollector.createTickerTypeMetrics(metricsConfigProcessor.getTickerTypes());
      metricsCollector.createHistogramTypeMetrics(metricsConfigProcessor.getHistogramTypes());
    }
    ShutdownManager shutdownManager = new ShutdownManager(server, roxDB, metricExporter);
    server.start();
    shutdownManager.register();
  }

  void blockUntilShutdown() throws InterruptedException {
    server.blockUntilShutdown();
  }

  RoxDBConfig getConfig() {
    return config;
  }

  RoxDBServer getServer() {
    return server;
  }
}
