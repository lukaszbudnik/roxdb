package com.github.lukaszbudnik.roxdb.application;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigurationValidatorTest {

  @TempDir Path tempDir;

  @Test
  void testValidDefaultConfiguration() {
    ConfigurationValidator validator = new ConfigurationValidator();

    EnvironmentConfigReader configReader = new EnvironmentConfigReader();
    RoxDBConfig roxDBConfig = configReader.readConfiguration(Map.of());

    assertDoesNotThrow(() -> validator.validateConfiguration(roxDBConfig));
  }

  @Test
  void testValidDbPath(@TempDir Path tempDir) {
    Path tempFile = tempDir.resolve("test.db");

    ConfigurationValidator validator = new ConfigurationValidator();

    assertDoesNotThrow(() -> validator.validateDbPath(tempFile.toString()));
  }

  @Test
  void testDbPathNullOrEmpty() {
    ConfigurationValidator validator = new ConfigurationValidator();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> validator.validateDbPath(null));
    assertEquals("Database path cannot be null or empty", exception.getMessage());

    exception = assertThrows(IllegalArgumentException.class, () -> validator.validateDbPath(""));
    assertEquals("Database path cannot be null or empty", exception.getMessage());
  }

  @Test
  void testDbPathPointsToFile() throws IOException {
    Path tempFile = createTempFile("test.db");

    ConfigurationValidator validator = new ConfigurationValidator();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> validator.validateDbPath(tempFile.toString()));
    assertEquals("Database path must be a directory", exception.getMessage());
  }

  @Test
  void testInvalidDbPath() {
    // Test non-existent path that can't be created
    String invalidPath = "/nonexistent/path/that/cant/be/created";

    ConfigurationValidator validator = new ConfigurationValidator();

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> validator.validateDbPath(invalidPath));
    assertEquals("Cannot create database directory", exception.getMessage());
  }

  @Test
  void testDbPathCreation(@TempDir Path tempDir) {
    String newDirPath = tempDir.resolve("newdir").toString();

    ConfigurationValidator validator = new ConfigurationValidator();
    validator.validateDbPath(newDirPath);

    File newDirFile = new File(newDirPath);
    assertTrue(newDirFile.exists());
    assertTrue(newDirFile.isDirectory());
  }

  @Test
  void testNonWritableDbPath(@TempDir Path tempDir) {
    File nonWritableDir = tempDir.toFile();
    nonWritableDir.setWritable(false);

    ConfigurationValidator validator = new ConfigurationValidator();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateDbPath(nonWritableDir.toString()));
    assertEquals("Database directory must be writable", exception.getMessage());
  }

  @Test
  void testValidPort() {
    ConfigurationValidator validator = new ConfigurationValidator();

    assertDoesNotThrow(() -> validator.validatePort(50501));
  }

  @Test
  void testInvalidPort() {
    ConfigurationValidator validator = new ConfigurationValidator();
    // Test negative port
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> validator.validatePort(-1));
    assertEquals("Invalid port number", exception.getMessage());

    // Test port zero
    exception = assertThrows(IllegalArgumentException.class, () -> validator.validatePort(0));
    assertEquals("Invalid port number", exception.getMessage());

    // Test port too high
    exception = assertThrows(IllegalArgumentException.class, () -> validator.validatePort(65536));
    assertEquals("Invalid port number", exception.getMessage());
  }

  @Test
  void testNoTLSConfiguration() {
    ConfigurationValidator validator = new ConfigurationValidator();
    // TLS configuration is optional and empty values are valid ones
    assertDoesNotThrow(() -> validator.validateTLSConfiguration("", "", ""));
  }

  @Test
  void testValidBasicTLSConfiguration() throws IOException {
    // Create temporary files for certificate and private key
    Path certPath = createTempFile("cert.pem");
    Path keyPath = createTempFile("key.pem");

    ConfigurationValidator validator = new ConfigurationValidator();

    assertDoesNotThrow(
        () -> validator.validateTLSConfiguration(certPath.toString(), keyPath.toString(), ""));
  }

  @Test
  void testValidMutualTLSConfiguration() throws IOException {
    // Create temporary files for all TLS components
    Path certPath = createTempFile("cert.pem");
    Path keyPath = createTempFile("key.pem");
    Path chainPath = createTempFile("chain.pem");

    ConfigurationValidator validator = new ConfigurationValidator();

    assertDoesNotThrow(
        () ->
            validator.validateTLSConfiguration(
                certPath.toString(), keyPath.toString(), chainPath.toString()));
  }

  @Test
  void testInvalidTLSConfiguration_CertificateWithoutKey() throws IOException {
    Path certPath = createTempFile("cert.pem");

    ConfigurationValidator validator = new ConfigurationValidator();
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateTLSConfiguration(certPath.toString(), "", ""));
    assertEquals(
        "TLS certificate path and TLS private key path must be set or both empty",
        exception.getMessage());
  }

  @Test
  void testInvalidTLSConfiguration_KeyWithoutCertificate() throws IOException {
    Path keyPath = createTempFile("key.pem");

    ConfigurationValidator validator = new ConfigurationValidator();
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateTLSConfiguration("", keyPath.toString(), ""));
    assertEquals(
        "TLS certificate path and TLS private key path must be set or both empty",
        exception.getMessage());
  }

  @Test
  void testInvalidTLSConfiguration_ChainWithoutBasicTLS() throws IOException {
    Path chainPath = createTempFile("chain.pem");

    ConfigurationValidator validator = new ConfigurationValidator();
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateTLSConfiguration("", "", chainPath.toString()));
    assertEquals(
        "TLS certificate chain path set but TLS certificate path or TLS private key path not set",
        exception.getMessage());
  }

  @Test
  void testInvalidTLSConfiguration_NonExistentCertFile() {
    String certPath = "/non/existent/cert.pem";

    ConfigurationValidator validator = new ConfigurationValidator();
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateTLSConfiguration(certPath, "", ""));
    assertEquals("TLS certificate file does not exist or cannot be read", exception.getMessage());
  }

  @Test
  void testInvalidTLSConfiguration_NonExistentKeyFile() throws IOException {
    Path certPath = createTempFile("cert.pem");
    String keyPath = "/non/existent/key.pem";

    ConfigurationValidator validator = new ConfigurationValidator();
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateTLSConfiguration(certPath.toString(), keyPath, ""));
    assertEquals("TLS private key file does not exist or cannot be read", exception.getMessage());
  }

  @Test
  void testInvalidTLSConfiguration_NonExistentChainFile() throws IOException {
    Path certPath = createTempFile("cert.pem");
    Path keyPath = createTempFile("key.pem");
    String chainPath = "/non/existent/chain.pem";

    ConfigurationValidator validator = new ConfigurationValidator();
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateTLSConfiguration(
                    certPath.toString(), keyPath.toString(), chainPath));
    assertEquals(
        "TLS certificate chain file does not exist or cannot be read", exception.getMessage());
  }

  @Test
  void testInvalidTLSConfiguration_UnreadableCertFile() throws IOException {
    Path certPath = createTempFileWithoutReadPermission("cert.pem");
    Path keyPath = createTempFile("key.pem");

    ConfigurationValidator validator = new ConfigurationValidator();
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateTLSConfiguration(certPath.toString(), keyPath.toString(), ""));
    assertEquals("TLS certificate file does not exist or cannot be read", exception.getMessage());
  }

  @Test
  void testInvalidTLSConfiguration_UnreadableKeyFile() throws IOException {
    Path certPath = createTempFile("cert.pem");
    Path keyPath = createTempFileWithoutReadPermission("key.pem");

    ConfigurationValidator validator = new ConfigurationValidator();
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateTLSConfiguration(certPath.toString(), keyPath.toString(), ""));
    assertEquals("TLS private key file does not exist or cannot be read", exception.getMessage());
  }

  @Test
  void testInvalidTLSConfiguration_UnreadableChainFile() throws IOException {
    Path certPath = createTempFile("cert.pem");
    Path keyPath = createTempFile("key.pem");
    Path chainPath = createTempFileWithoutReadPermission("chain.pem");

    ConfigurationValidator validator = new ConfigurationValidator();
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateTLSConfiguration(
                    certPath.toString(), keyPath.toString(), chainPath.toString()));
    assertEquals(
        "TLS certificate chain file does not exist or cannot be read", exception.getMessage());
  }

  private Path createTempFile(String fileName) throws IOException {
    Path filePath = tempDir.resolve(fileName);
    Files.createFile(filePath);
    return filePath;
  }

  private Path createTempFileWithoutReadPermission(String fileName) throws IOException {
    Path filePath = tempDir.resolve(fileName);
    Files.createFile(filePath);
    filePath.toFile().setReadable(false);
    return filePath;
  }
}
