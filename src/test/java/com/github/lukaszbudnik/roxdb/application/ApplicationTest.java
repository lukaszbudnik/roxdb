package com.github.lukaszbudnik.roxdb.application;

import static com.github.lukaszbudnik.roxdb.application.Application.*;
import static org.junit.jupiter.api.Assertions.*;

import com.github.lukaszbudnik.roxdb.api.RoxDBConfig;
import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ApplicationTest {

  @TempDir Path tempDir;

  private Map<String, String> envVars;

  @BeforeEach
  void setUp() {
    envVars = new HashMap<>();
  }

  @AfterEach
  void tearDown() {
    envVars.clear();
  }

  @Test
  void testDefaultConfiguration() {
    RoxDBConfig config = loadConfiguration(envVars);

    assertEquals(DEFAULT_PORT, config.port());
    assertEquals(DEFAULT_DB_PATH, config.dbPath());
  }

  @Test
  void testCustomPort() {
    envVars.put(ENV_PORT, "50052");

    RoxDBConfig config = loadConfiguration(envVars);

    assertEquals(50052, config.port());
    assertEquals(DEFAULT_DB_PATH, config.dbPath());
  }

  @Test
  void testCustomDbPath() {
    String customPath = tempDir.toString();
    envVars.put(ENV_DB_PATH, customPath);

    RoxDBConfig config = loadConfiguration(envVars);

    assertEquals(DEFAULT_PORT, config.port());
    assertEquals(customPath, config.dbPath());
  }

  @Test
  void testBothCustomValues() {
    String customPath = tempDir.toString();
    envVars.put(ENV_PORT, "50052");
    envVars.put(ENV_DB_PATH, customPath);

    RoxDBConfig config = loadConfiguration(envVars);

    assertEquals(50052, config.port());
    assertEquals(customPath, config.dbPath());
  }

  @Test
  void testInvalidPort() {
    // Test negative port
    envVars.put(ENV_PORT, "-1");
    assertThrows(IllegalArgumentException.class, () -> Application.loadConfiguration(envVars));

    // Test port zero
    envVars.put(ENV_PORT, "0");
    assertThrows(IllegalArgumentException.class, () -> Application.loadConfiguration(envVars));

    // Test port too high
    envVars.put(ENV_PORT, "65536");
    assertThrows(IllegalArgumentException.class, () -> Application.loadConfiguration(envVars));

    // Test non-numeric port
    envVars.put(ENV_PORT, "abc");
    RoxDBConfig config = loadConfiguration(envVars);
    assertEquals(DEFAULT_PORT, config.port(), "Should use default port for non-numeric value");
  }

  @Test
  void testInvalidDbPath() {
    // Test empty path
    envVars.put(ENV_DB_PATH, "");
    RoxDBConfig config = loadConfiguration(envVars);
    assertEquals(DEFAULT_DB_PATH, config.dbPath());

    // Test non-existent path that can't be created
    envVars.put(ENV_DB_PATH, "/nonexistent/path/that/cant/be/created");
    assertThrows(IllegalArgumentException.class, () -> Application.loadConfiguration(envVars));
  }

  @Test
  void testDbPathCreation(@TempDir Path tempDir) {
    String newDirPath = tempDir.resolve("newdir").toString();
    envVars.put(ENV_DB_PATH, newDirPath);

    RoxDBConfig config = loadConfiguration(envVars);

    assertTrue(new File(newDirPath).exists());
    assertTrue(new File(newDirPath).isDirectory());
  }

  @Test
  void testNonWritableDbPath(@TempDir Path tempDir) {
    File nonWritableDir = tempDir.toFile();
    nonWritableDir.setWritable(false);

    envVars.put(ENV_DB_PATH, nonWritableDir.getAbsolutePath());

    assertThrows(IllegalArgumentException.class, () -> Application.loadConfiguration(envVars));
  }
}
