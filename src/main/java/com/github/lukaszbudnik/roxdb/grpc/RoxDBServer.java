package com.github.lukaszbudnik.roxdb.grpc;

import com.github.lukaszbudnik.roxdb.api.RoxDB;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RoxDBServer {
    private static final Logger logger = LoggerFactory.getLogger(RoxDBServer.class);

    private final int port;
    private final RoxDB roxDB;
    private final Server server;

    public RoxDBServer(int port, RoxDB roxDB) {
        this.port = port;
        this.roxDB = roxDB;
        this.server = ServerBuilder.forPort(port)
                                   .addService(new RoxDBGrpcService(roxDB))
                                   .build();
    }

    public void start() throws IOException {
        logger.info("Starting server on port {}", port);

        server.start();

        logger.info("Server started, listening on port {}", port);
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            logger.info("Initiating graceful shutdown");
            server.shutdown();
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
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}