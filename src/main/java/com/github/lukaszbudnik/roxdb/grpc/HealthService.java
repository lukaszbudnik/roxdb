package com.github.lukaszbudnik.roxdb.grpc;

import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthService extends HealthGrpc.HealthImplBase {

  private final Logger logger = LoggerFactory.getLogger(HealthService.class);

  private final Map<String, HealthCheckResponse.ServingStatus> servingStatusMap = new HashMap<>();

  @Override
  public void check(
      HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseObserver) {
    String service = request.getService();
    HealthCheckResponse.ServingStatus servingStatus =
        servingStatusMap.getOrDefault(service, HealthCheckResponse.ServingStatus.SERVICE_UNKNOWN);
    logger.debug(
        "Returning health check for {} with service status: {}", service, servingStatus.name());
    responseObserver.onNext(HealthCheckResponse.newBuilder().setStatus(servingStatus).build());
    responseObserver.onCompleted();
  }

  public void setServiceStatus(String service, HealthCheckResponse.ServingStatus servingStatus) {
    logger.info("Setting {} service status to: {}", service, servingStatus.name());
    servingStatusMap.put(service, servingStatus);
  }
}
