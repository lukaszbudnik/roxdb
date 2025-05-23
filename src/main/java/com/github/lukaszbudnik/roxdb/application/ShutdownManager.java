package com.github.lukaszbudnik.roxdb.application;

import com.github.lukaszbudnik.roxdb.grpc.RoxDBServer;
import com.github.lukaszbudnik.roxdb.rocksdb.RoxDB;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShutdownManager {
  private final Logger logger = LoggerFactory.getLogger(ShutdownManager.class);
  private final Thread shutdownHook;
  private final RoxDBServer server;
  private final RoxDB roxDB;
  private final MetricExporter metricExporter;

  public ShutdownManager(RoxDBServer server, RoxDB roxDB, MetricExporter metricExporter) {
    this.server = server;
    this.roxDB = roxDB;
    this.metricExporter = metricExporter;
    this.shutdownHook = createShutdownHook();
  }

  private Thread createShutdownHook() {
    return new Thread(this::performShutdown);
  }

  void performShutdown() {
    logger.info("Shutting down server");
    try {
      server.stop();
    } catch (InterruptedException e) {
      logger.error("Error during server shutdown", e);
    }

    try {
      roxDB.close();
    } catch (Exception e) {
      logger.error("Error closing RocksDB instance", e);
    }

    if (metricExporter != null) {
      try {
        metricExporter.shutdown();
      } catch (Exception e) {
        logger.error("Error closing metric exporter", e);
      }
    }
  }

  public void register() {
    Runtime.getRuntime().addShutdownHook(shutdownHook);
  }
}
