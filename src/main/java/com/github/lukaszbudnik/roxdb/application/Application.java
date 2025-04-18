package com.github.lukaszbudnik.roxdb.application;

import com.github.lukaszbudnik.roxdb.rocksdb.RoxDB;
import com.github.lukaszbudnik.roxdb.rocksdb.RoxDBImpl;
import com.github.lukaszbudnik.roxdb.grpc.RoxDBServer;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {
  public static final String ENV_PORT = "ROXDB_PORT";
  public static final String ENV_DB_PATH = "ROXDB_DB_PATH";
  public static final int DEFAULT_PORT = 50051;
  public static final String DEFAULT_DB_PATH = "/tmp/rocksdb";
  private static final Logger logger = LoggerFactory.getLogger(Application.class);

  public static void main(String[] args) {
    try {
      logger.info("Starting RoxDB");
      RoxDBConfig config = loadConfiguration(System.getenv());
      RoxDB roxDB = initializeDatabase(config);
      RoxDBServer server = startServer(config, roxDB);
      setupShutdownHook(server, roxDB);
      server.blockUntilShutdown();
    } catch (Exception e) {
      logger.error("Failed to start RoxDB", e);
      System.exit(1);
    }
  }

  static RoxDBConfig loadConfiguration(Map<String, String> env) {
    int port = DEFAULT_PORT;
    String dbPath = DEFAULT_DB_PATH;

    // Try to get port from environment
    String envPort = env.get(ENV_PORT);
    if (envPort != null && !envPort.isBlank()) {
      try {
        port = Integer.parseInt(envPort);
        logger.info("Using port from environment variable: {}", port);
      } catch (NumberFormatException e) {
        logger.warn(
            "Invalid port number in environment variable {}: '{}'. Using default: {}",
            ENV_PORT,
            envPort,
            DEFAULT_PORT);
      }
    } else {
      logger.info("No port specified in environment. Using default: {}", DEFAULT_PORT);
    }

    // Try to get db path from environment
    String envDbPath = env.get(ENV_DB_PATH);
    if (envDbPath != null && !envDbPath.isBlank()) {
      dbPath = envDbPath;
      logger.info("Using database path from environment variable: {}", dbPath);
    } else {
      logger.info("No database path specified in environment. Using default: {}", DEFAULT_DB_PATH);
    }

    // Validate configuration
    validateConfiguration(port, dbPath);

    return new RoxDBConfig(port, dbPath);
  }

  private static void validateConfiguration(int port, String dbPath) {
    // Validate port
    if (port <= 0 || port > 65535) {
      logger.error("Invalid port number: {}. Port must be between 1 and 65535", port);
      throw new IllegalArgumentException("Invalid port number");
    }

    // Validate db path
    if (dbPath == null || dbPath.trim().isEmpty()) {
      logger.error("Database path cannot be null or empty");
      throw new IllegalArgumentException("Invalid database path");
    }

    // Check if directory exists or can be created
    File dbDir = new File(dbPath);
    if (!dbDir.exists()) {
      logger.info("Database directory does not exist, attempting to create: {}", dbPath);
      if (!dbDir.mkdirs()) {
        logger.error("Failed to create database directory: {}", dbPath);
        throw new IllegalArgumentException("Cannot create database directory");
      }
    } else if (!dbDir.isDirectory()) {
      logger.error("Database path exists but is not a directory: {}", dbPath);
      throw new IllegalArgumentException("Database path must be a directory");
    }

    // Check if directory is writable
    if (!dbDir.canWrite()) {
      logger.error("Database directory is not writable: {}", dbPath);
      throw new IllegalArgumentException("Database directory must be writable");
    }
  }

  private static RoxDB initializeDatabase(RoxDBConfig config) throws RocksDBException {
    return new RoxDBImpl(config.dbPath());
  }

  private static RoxDBServer startServer(RoxDBConfig config, RoxDB roxDB) throws IOException {
    RoxDBServer server = new RoxDBServer(config.port(), roxDB);
    server.start();
    return server;
  }

  private static void setupShutdownHook(RoxDBServer server, RoxDB roxDB) {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
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
                }));
  }
}
