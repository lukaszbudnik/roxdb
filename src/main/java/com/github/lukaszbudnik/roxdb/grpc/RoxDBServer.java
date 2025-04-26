package com.github.lukaszbudnik.roxdb.grpc;

import com.github.lukaszbudnik.roxdb.application.RoxDBConfig;
import com.github.lukaszbudnik.roxdb.v1.RoxDBGrpc;
import com.google.common.base.Strings;
import io.grpc.*;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoxDBServer {
  private static final Logger logger = LoggerFactory.getLogger(RoxDBServer.class);

  private final int port;
  private final RoxDBGrpcService roxDBGrpcService;
  private final Server server;
  private final HealthService healthService;

  public RoxDBServer(RoxDBConfig config, RoxDBGrpcService roxDBGrpcService) throws IOException {
    this.port = config.port();
    this.roxDBGrpcService = roxDBGrpcService;
    this.healthService = new HealthService();

    ServerCredentials serverCredentials = getServerCredentials(config);

    this.server =
        Grpc.newServerBuilderForPort(port, serverCredentials)
            .addService(roxDBGrpcService)
            .addService(healthService)
            .addService(ProtoReflectionServiceV1.newInstance())
            .build();
  }

  private ServerCredentials getServerCredentials(RoxDBConfig config) throws IOException {
    ServerCredentials serverCredentials;
    if (!Strings.isNullOrEmpty(config.tlsCertificatePath())
        && !Strings.isNullOrEmpty(config.tlsPrivateKeyPath())) {
      TlsServerCredentials.Builder tlsBuilder =
          TlsServerCredentials.newBuilder()
              .keyManager(
                  new File(config.tlsCertificatePath()), new File(config.tlsPrivateKeyPath()));
      if (!Strings.isNullOrEmpty(config.tlsCertificateChainPath())) {
        tlsBuilder.trustManager(new File(config.tlsCertificateChainPath()));
        tlsBuilder.clientAuth(TlsServerCredentials.ClientAuth.REQUIRE);
        logger.info("TLS mutual authentication enabled");
      } else {
        logger.info("TLS enabled");
      }
      serverCredentials = tlsBuilder.build();
    } else {
      serverCredentials = InsecureServerCredentials.create();
      logger.warn("TLS disabled - server will run in plaintext");
    }
    return serverCredentials;
  }

  public void start() throws IOException {
    logger.info("Starting server on port {}", port);

    server.start();

    setServiceStatus(ServingStatus.SERVING);

    logger.info("Server started, listening on port {}", port);
  }

  public void stop() throws InterruptedException {
    logger.info("Initiating graceful shutdown");
    server.shutdown();
    setServiceStatus(ServingStatus.NOT_SERVING);
    try {
      if (!server.awaitTermination(30, TimeUnit.SECONDS)) {
        logger.warn("Server did not terminate in 30 seconds. Forcing shutdown.");
        server.shutdownNow();
      }
    } catch (InterruptedException e) {
      logger.error("Server shutdown interrupted", e);
      server.shutdownNow();
      throw e;
    }
    logger.info("Server shutdown completed");
  }

  public void blockUntilShutdown() throws InterruptedException {
    server.awaitTermination();
  }

  public void setServiceStatus(ServingStatus servingStatus) {
    String service = RoxDBGrpc.getServiceDescriptor().getName();
    healthService.setServiceStatus(service, servingStatus);
  }

  public ServingStatus getServiceStatus() {
    String service = RoxDBGrpc.getServiceDescriptor().getName();
    return healthService.getServiceStatus(service);
  }
}
