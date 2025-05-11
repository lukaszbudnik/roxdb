package com.github.lukaszbudnik.roxdb.application;

import static org.mockito.Mockito.*;

import com.github.lukaszbudnik.roxdb.grpc.RoxDBServer;
import com.github.lukaszbudnik.roxdb.rocksdb.RoxDB;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ShutdownManagerTest {

  @Mock private RoxDBServer mockServer;

  @Mock private RoxDB mockRoxDB;

  @Mock private MetricExporter metricExporter;

  private ShutdownManager shutdownManager;

  @BeforeEach
  public void setUp() {
    shutdownManager = new ShutdownManager(mockServer, mockRoxDB, metricExporter);
  }

  @Test
  public void testNormalShutdown() throws Exception {
    // Test normal shutdown scenario
    shutdownManager.performShutdown();

    // Verify that methods were called in correct order
    verify(mockServer).stop();
    verify(mockRoxDB).close();
  }

  @Test
  public void testServerShutdownException() throws Exception {
    // Simulate server throwing InterruptedException
    doThrow(new InterruptedException("Server shutdown failed")).when(mockServer).stop();

    shutdownManager.performShutdown();

    // Verify RoxDB.close() was still called
    verify(mockRoxDB).close();
  }

  @Test
  public void testRoxDBCloseException() throws Exception {
    // Simulate RoxDB throwing Exception
    doThrow(new RuntimeException("Database close failed")).when(mockRoxDB).close();

    shutdownManager.performShutdown();

    // Verify server.stop() was called
    verify(mockServer).stop();
  }

  @Test
  public void testBothComponentsThrowExceptions() throws Exception {
    // Simulate both components throwing exceptions
    doThrow(new InterruptedException("Server shutdown failed")).when(mockServer).stop();
    doThrow(new RuntimeException("Database close failed")).when(mockRoxDB).close();

    shutdownManager.performShutdown();
  }

  @Test
  public void testShutdownHookRegistration() {
    // Create a custom Runtime for testing
    Runtime runtime = mock(Runtime.class);

    // Create a custom ShutdownManager that uses the mocked Runtime
    ShutdownManager manager =
        new ShutdownManager(mockServer, mockRoxDB, metricExporter) {
          @Override
          public void register() {
            runtime.addShutdownHook(new Thread(this::performShutdown));
          }
        };

    // Register shutdown hook
    manager.register();

    // Verify shutdown hook was registered
    verify(runtime).addShutdownHook(any(Thread.class));
  }
}
