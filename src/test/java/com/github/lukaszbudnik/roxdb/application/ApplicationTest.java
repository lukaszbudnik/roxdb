package com.github.lukaszbudnik.roxdb.application;

import static com.github.lukaszbudnik.roxdb.application.Application.*;
import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ApplicationTest {

  @TempDir Path tempDir;

  @Test
  void testDefaultConfiguration() {
    RoxDBConfig config = loadConfiguration(Map.of());

    assertEquals(DEFAULT_PORT, config.port());
    assertEquals(DEFAULT_DB_PATH, config.dbPath());
  }

  @Test
  void testCustomPort() {
    Map<String, String> env = new HashMap<>();
    env.put(ENV_PORT, "50052");

    RoxDBConfig config = loadConfiguration(env);

    assertEquals(50052, config.port());
    assertEquals(DEFAULT_DB_PATH, config.dbPath());
  }

  @Test
  void testCustomDbPath() {
    Map<String, String> env = new HashMap<>();
    String customPath = tempDir.toString();
    env.put(ENV_DB_PATH, customPath);

    RoxDBConfig config = loadConfiguration(env);

    assertEquals(DEFAULT_PORT, config.port());
    assertEquals(customPath, config.dbPath());
  }

  @Test
  void testBothCustomValues() {
    Map<String, String> env = new HashMap<>();
    String customPath = tempDir.toString();
    env.put(ENV_PORT, "50052");
    env.put(ENV_DB_PATH, customPath);

    RoxDBConfig config = loadConfiguration(env);

    assertEquals(50052, config.port());
    assertEquals(customPath, config.dbPath());
  }

  // test error when dbPath points to a file
  @Test
  void testDbPathPointsToFile() throws IOException {
    Map<String, String> env = new HashMap<>();
    Path tempFile = createTempFile("test.db");
    env.put(ENV_DB_PATH, tempFile.toString());

    assertThrows(IllegalArgumentException.class, () -> Application.loadConfiguration(env));
  }

  @Test
  void testInvalidPort() {
    Map<String, String> env = new HashMap<>();
    // Test negative port
    env.put(ENV_PORT, "-1");
    assertThrows(IllegalArgumentException.class, () -> Application.loadConfiguration(env));

    // Test port zero
    env.put(ENV_PORT, "0");
    assertThrows(IllegalArgumentException.class, () -> Application.loadConfiguration(env));

    // Test port too high
    env.put(ENV_PORT, "65536");
    assertThrows(IllegalArgumentException.class, () -> Application.loadConfiguration(env));

    // Test non-numeric port
    env.put(ENV_PORT, "abc");
    RoxDBConfig config = loadConfiguration(env);
    assertEquals(DEFAULT_PORT, config.port(), "Should use default port for non-numeric value");
  }

  @Test
  void testInvalidDbPath() {
    Map<String, String> env = new HashMap<>();
    // Test empty path
    env.put(ENV_DB_PATH, "");
    RoxDBConfig config = loadConfiguration(env);
    assertEquals(DEFAULT_DB_PATH, config.dbPath());

    // Test non-existent path that can't be created
    env.put(ENV_DB_PATH, "/nonexistent/path/that/cant/be/created");
    assertThrows(IllegalArgumentException.class, () -> Application.loadConfiguration(env));
  }

  @Test
  void testDbPathCreation(@TempDir Path tempDir) {
    Map<String, String> env = new HashMap<>();
    String newDirPath = tempDir.resolve("newdir").toString();
    env.put(ENV_DB_PATH, newDirPath);

    RoxDBConfig config = loadConfiguration(env);

    assertTrue(new File(newDirPath).exists());
    assertTrue(new File(newDirPath).isDirectory());
  }

  @Test
  void testNonWritableDbPath(@TempDir Path tempDir) {
    Map<String, String> env = new HashMap<>();
    File nonWritableDir = tempDir.toFile();
    nonWritableDir.setWritable(false);

    env.put(ENV_DB_PATH, nonWritableDir.getAbsolutePath());

    assertThrows(IllegalArgumentException.class, () -> Application.loadConfiguration(env));
  }

  @Test
  void testNoTLSConfiguration() {
    Map<String, String> env = new HashMap<>();
    RoxDBConfig config = Application.loadConfiguration(env);

    assertNull(config.tlsCertificatePath());
    assertNull(config.tlsPrivateKeyPath());
    assertNull(config.tlsCertificateChainPath());
  }

  @Test
  void testValidBasicTLSConfiguration() throws IOException {
    // Create temporary files for certificate and private key
    Path certPath = createTempFile("cert.pem");
    Path keyPath = createTempFile("key.pem");

    Map<String, String> env = new HashMap<>();
    env.put(Application.ENV_TLS_CERTIFICATE_PATH, certPath.toString());
    env.put(Application.ENV_TLS_PRIVATE_KEY_PATH, keyPath.toString());

    RoxDBConfig config = Application.loadConfiguration(env);

    assertEquals(certPath.toString(), config.tlsCertificatePath());
    assertEquals(keyPath.toString(), config.tlsPrivateKeyPath());
    assertNull(config.tlsCertificateChainPath());
  }

  @Test
  void testValidMutualTLSConfiguration() throws IOException {
    // Create temporary files for all TLS components
    Path certPath = createTempFile("cert.pem");
    Path keyPath = createTempFile("key.pem");
    Path chainPath = createTempFile("chain.pem");

    Map<String, String> env = new HashMap<>();
    env.put(Application.ENV_TLS_CERTIFICATE_PATH, certPath.toString());
    env.put(Application.ENV_TLS_PRIVATE_KEY_PATH, keyPath.toString());
    env.put(Application.ENV_TLS_CERTIFICATE_CHAIN_PATH, chainPath.toString());

    RoxDBConfig config = Application.loadConfiguration(env);

    assertEquals(certPath.toString(), config.tlsCertificatePath());
    assertEquals(keyPath.toString(), config.tlsPrivateKeyPath());
    assertEquals(chainPath.toString(), config.tlsCertificateChainPath());
  }

  @Test
  void testInvalidTLSConfiguration_CertificateWithoutKey() throws IOException {
    Path certPath = createTempFile("cert.pem");

    Map<String, String> env = new HashMap<>();
    env.put(Application.ENV_TLS_CERTIFICATE_PATH, certPath.toString());

    assertThrows(IllegalArgumentException.class, () -> Application.loadConfiguration(env));
  }

  @Test
  void testInvalidTLSConfiguration_KeyWithoutCertificate() throws IOException {
    Path keyPath = createTempFile("key.pem");

    Map<String, String> env = new HashMap<>();
    env.put(Application.ENV_TLS_PRIVATE_KEY_PATH, keyPath.toString());

    assertThrows(IllegalArgumentException.class, () -> Application.loadConfiguration(env));
  }

  @Test
  void testInvalidTLSConfiguration_ChainWithoutBasicTLS() throws IOException {
    Path chainPath = createTempFile("chain.pem");

    Map<String, String> env = new HashMap<>();
    env.put(Application.ENV_TLS_CERTIFICATE_CHAIN_PATH, chainPath.toString());

    assertThrows(IllegalArgumentException.class, () -> Application.loadConfiguration(env));
  }

  @Test
  void testInvalidTLSConfiguration_NonExistentFiles() {
    Map<String, String> env = new HashMap<>();
    env.put(Application.ENV_TLS_CERTIFICATE_PATH, "/non/existent/cert.pem");
    env.put(Application.ENV_TLS_PRIVATE_KEY_PATH, "/non/existent/key.pem");

    assertThrows(IllegalArgumentException.class, () -> Application.loadConfiguration(env));
  }

  @Test
  void testInvalidTLSConfiguration_UnreadableFiles() throws IOException {
    Path certPath = createTempFile("cert.pem");
    Path keyPath = createTempFileWithoutReadPermission("key.pem");

    Map<String, String> env = new HashMap<>();
    env.put(Application.ENV_TLS_CERTIFICATE_PATH, certPath.toString());
    env.put(Application.ENV_TLS_PRIVATE_KEY_PATH, keyPath.toString());

    assertThrows(IllegalArgumentException.class, () -> Application.loadConfiguration(env));
  }

  @Test
  void testInvalidTLSConfiguration_UnreadableCAChainFiles() throws IOException {
    Path certPath = createTempFile("cert.pem");
    Path keyPath = createTempFile("key.pem");
    Path chainPath = createTempFileWithoutReadPermission("chain.pem");

    Map<String, String> env = new HashMap<>();
    env.put(Application.ENV_TLS_CERTIFICATE_PATH, certPath.toString());
    env.put(Application.ENV_TLS_PRIVATE_KEY_PATH, keyPath.toString());
    env.put(Application.ENV_TLS_CERTIFICATE_CHAIN_PATH, chainPath.toString());

    assertThrows(IllegalArgumentException.class, () -> Application.loadConfiguration(env));
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
