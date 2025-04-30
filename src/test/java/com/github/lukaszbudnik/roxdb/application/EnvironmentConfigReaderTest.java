package com.github.lukaszbudnik.roxdb.application;

import static com.github.lukaszbudnik.roxdb.application.EnvironmentConfigReader.*;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EnvironmentConfigReaderTest {

  @TempDir Path tempDir;

  @Test
  void testDefaultConfiguration() {
    EnvironmentConfigReader configReader = new EnvironmentConfigReader();
    RoxDBConfig config = configReader.readConfiguration(Map.of());

    assertEquals(DEFAULT_PORT, config.port());
    assertEquals(DEFAULT_DB_PATH, config.dbPath());
  }

  @Test
  void testCustomPort() {
    Map<String, String> env = new HashMap<>();
    env.put(ENV_PORT, "50052");

    EnvironmentConfigReader configReader = new EnvironmentConfigReader();
    RoxDBConfig config = configReader.readConfiguration(env);

    assertEquals(50052, config.port());
    assertEquals(DEFAULT_DB_PATH, config.dbPath());
  }

  @Test
  void testNonNumericPort() {
    Map<String, String> env = new HashMap<>();
    env.put(ENV_PORT, "abc");

    EnvironmentConfigReader configReader = new EnvironmentConfigReader();
    RoxDBConfig config = configReader.readConfiguration(env);

    assertEquals(DEFAULT_PORT, config.port());
  }

  @Test
  void testCustomDbPath() {
    Map<String, String> env = new HashMap<>();
    String customPath = tempDir.toString();
    env.put(ENV_DB_PATH, customPath);

    EnvironmentConfigReader configReader = new EnvironmentConfigReader();
    RoxDBConfig config = configReader.readConfiguration(env);

    assertEquals(DEFAULT_PORT, config.port());
    assertEquals(customPath, config.dbPath());
  }

  @Test
  void testBothCustomValues() {
    Map<String, String> env = new HashMap<>();
    String customPath = tempDir.toString();
    env.put(ENV_PORT, "50052");
    env.put(ENV_DB_PATH, customPath);

    EnvironmentConfigReader configReader = new EnvironmentConfigReader();
    RoxDBConfig config = configReader.readConfiguration(env);

    assertEquals(50052, config.port());
    assertEquals(customPath, config.dbPath());
  }

  @Test
  void testTLSConfiguration() {
    Map<String, String> env = new HashMap<>();
    env.put(ENV_TLS_PRIVATE_KEY_PATH, "key.pem");
    env.put(ENV_TLS_CERTIFICATE_PATH, "cert.pem");
    env.put(ENV_TLS_CERTIFICATE_CHAIN_PATH, "ca_cert.pem");

    EnvironmentConfigReader configReader = new EnvironmentConfigReader();
    RoxDBConfig config = configReader.readConfiguration(env);

    // assert
    assertEquals("key.pem", config.tlsPrivateKeyPath());
    assertEquals("cert.pem", config.tlsCertificatePath());
    assertEquals("ca_cert.pem", config.tlsCertificateChainPath());
  }
}
