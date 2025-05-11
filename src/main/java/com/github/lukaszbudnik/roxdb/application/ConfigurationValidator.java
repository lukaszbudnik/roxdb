package com.github.lukaszbudnik.roxdb.application;

import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationValidator {
  private final Logger logger = LoggerFactory.getLogger(ConfigurationValidator.class);

  public void validateConfiguration(RoxDBConfig config) {
    validatePort(config.port());
    validateDbPath(config.dbPath());
    validateFile(config.openTelemetryConfig(), "OpenTelemetry config");
    validateTLSConfiguration(
        config.tlsCertificatePath(), config.tlsPrivateKeyPath(), config.tlsCertificateChainPath());
  }

  void validatePort(int port) {
    if (port <= 0 || port > 65535) {
      logger.error("Invalid port number: {}. Port must be between 1 and 65535", port);
      throw new IllegalArgumentException("Invalid port number");
    }
  }

  void validateDbPath(String dbPath) {
    if (dbPath == null || dbPath.trim().isEmpty()) {
      logger.error("Database path cannot be null or empty");
      throw new IllegalArgumentException("Database path cannot be null or empty");
    }

    File dbDir = new File(dbPath);
    validateDbDirectory(dbDir);
    validateDbDirectoryPermissions(dbDir);
  }

  private void validateDbDirectory(File dbDir) {
    if (!dbDir.exists()) {
      logger.info("Database directory does not exist, attempting to create: {}", dbDir.getPath());
      if (!dbDir.mkdirs()) {
        logger.error("Failed to create database directory: {}", dbDir.getPath());
        throw new IllegalArgumentException("Cannot create database directory");
      }
    } else if (!dbDir.isDirectory()) {
      logger.error("Database path exists but is not a directory: {}", dbDir.getPath());
      throw new IllegalArgumentException("Database path must be a directory");
    }
  }

  private void validateDbDirectoryPermissions(File dbDir) {
    if (!dbDir.canWrite()) {
      logger.error("Database directory is not writable: {}", dbDir.getPath());
      throw new IllegalArgumentException("Database directory must be writable");
    }
  }

  void validateTLSConfiguration(
      String tlsCertificatePath, String tlsPrivateKeyPath, String tlsCertificateChainPath) {

    boolean tlsCertificatePathSet = validateFile(tlsCertificatePath, "TLS certificate");
    boolean tlsPrivateKeyPathSet = validateFile(tlsPrivateKeyPath, "TLS private key");
    boolean tlsCertificateChainPathSet =
        validateFile(tlsCertificateChainPath, "TLS certificate chain");

    validateTLSCombination(tlsCertificatePathSet, tlsPrivateKeyPathSet, tlsCertificateChainPathSet);
  }

  private boolean validateFile(String filePath, String fileType) {
    if (filePath == null || filePath.isBlank()) {
      return false;
    }

    File file = new File(filePath);
    if (!file.exists() || !file.canRead()) {
      logger.error("{} file does not exist or cannot be read: {}", fileType, filePath);
      throw new IllegalArgumentException(fileType + " file does not exist or cannot be read");
    }
    return true;
  }

  private void validateTLSCombination(
      boolean tlsCertificatePathSet,
      boolean tlsPrivateKeyPathSet,
      boolean tlsCertificateChainPathSet) {

    // Check if certificate and private key are properly paired
    if (tlsPrivateKeyPathSet != tlsCertificatePathSet) {
      logger.error(
          "TLS certificate path and TLS private key path must be set or both empty. "
              + "tlsCertificatePath set: {}, tlsPrivateKeyPath set: {}",
          tlsCertificatePathSet,
          tlsPrivateKeyPathSet);
      throw new IllegalArgumentException(
          "TLS certificate path and TLS private key path must be set or both empty");
    }

    // Check certificate chain configuration
    if (tlsCertificateChainPathSet && (!tlsCertificatePathSet || !tlsPrivateKeyPathSet)) {
      logger.error(
          "TLS certificate chain path set but TLS certificate path or TLS private key path "
              + "not set. tlsCertificatePath set: {}, tlsPrivateKeyPath set: {}, "
              + "tlsCertificateChainPath set: {}",
          tlsCertificatePathSet,
          tlsPrivateKeyPathSet,
          tlsCertificateChainPathSet);
      throw new IllegalArgumentException(
          "TLS certificate chain path set but TLS certificate path or TLS private key path not set");
    }
  }
}
