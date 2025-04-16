# Use Amazon Corretto 21 headless as the base image
FROM public.ecr.aws/amazoncorretto/amazoncorretto:21-al2023-headless

# Define build argument for JAR version
ARG ROXDB_VERSION=1.0-SNAPSHOT

RUN dnf upgrade -y && \
    dnf install -y shadow-utils && \
    dnf clean all && \
    rm -rf /var/cache/dnf

# Set working directory
WORKDIR /app

# Add a non-root user for security
RUN useradd -r -s /bin/false appuser

# Copy the JAR file
COPY build/libs/roxdb-${ROXDB_VERSION}-all.jar roxdb.jar

# Set ownership of the application files
RUN chown -R appuser:appuser /app

# Set environment variables
ENV ROXDB_DB_PATH=/tmp/rocksdb
ENV JAVA_OPTS="-Xms512m -Xmx512m"

# Expose the application port
EXPOSE 50051

# Switch to non-root user
USER appuser

# Start the application
ENTRYPOINT ["sh", "-c", "java -jar roxdb.jar"]
