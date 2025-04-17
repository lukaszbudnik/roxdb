# RoxDB

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![Java CI](https://github.com/lukaszbudnik/roxdb/actions/workflows/gradle.yml/badge.svg)](https://github.com/lukaszbudnik/roxdb/actions/workflows/gradle.yml) [![Docker Image CI](https://github.com/lukaszbudnik/roxdb/actions/workflows/docker-image.yml/badge.svg)](https://github.com/lukaszbudnik/roxdb/actions/workflows/docker-image.yml) [![GitHub Issues](https://img.shields.io/github/issues/lukaszbudnik/roxdb)](https://github.com/lukaszbudnik/roxdb/issues)

**DynamoDB-like implementation on RocksDB over gRPC.**

## Project Description

RoxDB is an open-source project that aims to provide a basic DynamoDB-like API using RocksDB as the storage engine. Instead of the standard HTTP JSON API, RoxDB leverages gRPC for efficient and high-performance communication.

This project is primarily focused on:

1.  **DynamoDB-like API Implementation:** Implementing core DynamoDB-like functionalities using RocksDB for persistent storage.
2.  **gRPC Integration:** Utilizing gRPC for communication, offering advantages like binary serialization, strong typing, and improved performance compared to HTTP JSON.
3.  **Code Generation with Amazon Q:** Leveraging Amazon Q for code generation to accelerate development and ensure consistency.
4.  **Consultation with Google Gemini:** Leveraging Google Gemini for consultations on software engineering best practices.

## Motivation

This project was born out of the desire to explore alternative storage solutions for DynamoDB-like workloads and to experiment with the benefits of gRPC in data-intensive applications. It also serves as a practical demonstration of using AI-powered code generation tools like Amazon Q to streamline development.

## Features

* **DynamoDB-like API:** `PutItem`, `UpdateItem`, `DeleteItem`, `GetItem`, `Query`.
* **RocksDB Storage:** Utilizing RocksDB for fast and reliable data storage.
* **gRPC Interface:** Providing a high-performance gRPC API for client interactions.
* **Efficient Data Serialization:** Leveraging gRPC's Protocol Buffers for efficient data serialization.
* **Containerization & Orchestration:** multi-arch (`linux/amd64` and `linux/arm64`) Docker container support for easy deployment, quick-start Kubernetes manifests provided

## Getting Started

### Prerequisites

* Java 21 or later
* Gradle
* Protocol Buffer Compiler (`protoc`)

### Installation

1.  **Clone the repository:**

    ```bash
    git clone git@github.com:lukaszbudnik/roxdb.git
    cd roxdb
    ```

2.  **Build the project:**

    ```bash
    ./gradlew build
    ```

### Running the Server

1.  **Start the RoxDB Java app:**

    ```bash
    # Run with defaults
    java -jar build/libs/roxdb-1.0-SNAPSHOT-all.jar
    # Run with custom port
    ROXDB_PORT=50052 java -jar build/libs/roxdb-1.0-SNAPSHOT-all.jar
    # Run with custom db path
    ROXDB_DB_PATH=/data/roxdb java -jar build/libs/roxdb-1.0-SNAPSHOT-all.jar
    # Run with both custom port and db path
    ROXDB_PORT=50052 ROXDB_DB_PATH=/data/roxdb java -jar build/libs/roxdb-1.0-SNAPSHOT-all.jar
    ```

2.  **Or build and start the RoxDB container:**

    ```bash
    # Build the Docker image and pass ROXDB_VERSION build argument
    docker build --build-arg ROXDB_VERSION=1.0-SNAPSHOT -t roxdb .
    # Run with default db path
    docker run -P roxdb
    # Run with custom db path/volume
    docker run -P -e ROXDB_DB_PATH=/data/roxdb roxdb
    ```

3.  **Or deploy RoxDB service to Kubernetes:**

    See [kubernetes/README.md](kubernetes/README.md).

4.  **Test:**

    Using [grpcurl](https://github.com/fullstorydev/grpcurl):

    ```bash
    export ROXDB_ENDPOINT=localhost:50051
    # Describe the service
    grpcurl -plaintext ${ROXDB_ENDPOINT} describe com.github.lukaszbudnik.roxdb.v1.RoxDB
    # Check service health
    grpcurl -plaintext -d '{"service": "com.github.lukaszbudnik.roxdb.v1.RoxDB"}' ${ROXDB_ENDPOINT} grpc.health.v1.Health/Check
    # Stream PutItem, UpdateItem, GetItem, DeleteItem, and one more GetItem in a single call
    grpcurl -d @ -plaintext ${ROXDB_ENDPOINT} com.github.lukaszbudnik.roxdb.v1.RoxDB/ProcessItems << EOM
    {
      "correlation_id": "123",
      "table": "your-table-name",
      "put_item": {
        "item": {
          "key": {
            "partition_key": "part1",
            "sort_key": "sort1"
          },
          "attributes": {
            "field1": "value1",
            "field2": 123
          }
        }
      }
    }
    {
      "correlation_id": "124",
      "table": "your-table-name",
      "update_item": {
        "item": {
          "key": {
            "partition_key": "part1",
            "sort_key": "sort1"
          },
          "attributes": {
            "field1": "new value for field1",
            "field3": "brand new field"
          }
        }
      }
    }
    {
      "correlation_id": "125",
      "table": "your-table-name",
      "get_item": {
        "key": {
          "partition_key": "part1",
          "sort_key": "sort1"
        }
      }
    }
    {
      "correlation_id": "126",
      "table": "your-table-name",
      "delete_item": {
        "key": {
          "partition_key": "part1",
          "sort_key": "sort1"
        }
      }
    }
    {
      "correlation_id": "127",
      "table": "your-table-name",
      "get_item": {
        "key": {
          "partition_key": "part1",
          "sort_key": "sort1"
        }
      }
    }
    EOM
    ```

## Contributing

Contributions are welcome! Please follow these steps:

1.  Fork the repository.
2.  Create a new branch for your feature or bug fix.
3.  Commit your changes.
4.  Push to your branch.
5.  Submit a pull request.

## License

This project is licensed under the Apache 2.0 License. See the `LICENSE` file for details.

## Acknowledgments

* [RocksDB](https://rocksdb.org/)
* [gRPC](https://grpc.io/)
* [Amazon Q](https://aws.amazon.com/amazon-q/)
* [Google Gemini](https://gemini.google.com)

## Future Work

* Future features or improvements: global secondary indexes, enhanced consistency.
* Add more DynamoDB API implementations: `TransactWriteItems`.
* Improve performance and scalability.
