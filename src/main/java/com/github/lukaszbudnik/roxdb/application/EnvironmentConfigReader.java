package com.github.lukaszbudnik.roxdb.application;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvironmentConfigReader {
  public static final String ENV_PORT = "ROXDB_PORT";
  public static final String ENV_DB_PATH = "ROXDB_DB_PATH";
  public static final String ENV_TLS_PRIVATE_KEY_PATH = "ROXDB_TLS_PRIVATE_KEY_PATH";
  public static final String ENV_TLS_CERTIFICATE_PATH = "ROXDB_TLS_CERTIFICATE_PATH";
  public static final String ENV_TLS_CERTIFICATE_CHAIN_PATH = "ROXDB_TLS_CERTIFICATE_CHAIN_PATH";

  public static final int DEFAULT_PORT = 50051;
  public static final String DEFAULT_DB_PATH = "/tmp/rocksdb";

  private static final Logger logger = LoggerFactory.getLogger(EnvironmentConfigReader.class);

  public RoxDBConfig readConfiguration(Map<String, String> env) {
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

    // Try to get TLS_CERTIFICATE_PATH from environment
    String tlsCertificatePath = env.get(ENV_TLS_CERTIFICATE_PATH);
    if (tlsCertificatePath != null && !tlsCertificatePath.isBlank()) {
      logger.info("Using TLS certificate path from environment variable: {}", tlsCertificatePath);
    } else {
      logger.info("No TLS certificate path specified in environment.");
    }

    // Try to get TLS_PRIVATE_KEY_PATH from environment
    String tlsPrivateKeyPath = env.get(ENV_TLS_PRIVATE_KEY_PATH);
    if (tlsPrivateKeyPath != null && !tlsPrivateKeyPath.isBlank()) {
      logger.info("Using TLS private key path from environment variable: {}", tlsPrivateKeyPath);
    } else {
      logger.info("No TLS private key path specified in environment.");
    }

    // Try to get TLS_CERTIFICATE_CHAIN_PATH from environment
    String tlsCertificateChainPath = env.get(ENV_TLS_CERTIFICATE_CHAIN_PATH);
    if (tlsCertificateChainPath != null && !tlsCertificateChainPath.isBlank()) {
      logger.info(
          "Using TLS certificate chain path from environment variable: {}",
          tlsCertificateChainPath);
    } else {
      logger.info("No TLS certificate chain path specified in environment.");
    }

    return new RoxDBConfig(
        port, dbPath, tlsCertificatePath, tlsPrivateKeyPath, tlsCertificateChainPath);
  }
}
