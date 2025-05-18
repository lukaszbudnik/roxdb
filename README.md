# RoxDB

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![Java CI](https://github.com/lukaszbudnik/roxdb/actions/workflows/gradle.yml/badge.svg)](https://github.com/lukaszbudnik/roxdb/actions/workflows/gradle.yml) [![Docker Image CI](https://github.com/lukaszbudnik/roxdb/actions/workflows/docker-image.yml/badge.svg)](https://github.com/lukaszbudnik/roxdb/actions/workflows/docker-image.yml) [![GitHub Issues](https://img.shields.io/github/issues/lukaszbudnik/roxdb)](https://github.com/lukaszbudnik/roxdb/issues)

**DynamoDB-like implementation on RocksDB over gRPC.**

## Project Description

RoxDB is an open-source project that aims to provide a basic DynamoDB-like API using RocksDB as the storage engine.
Instead of the standard HTTP JSON API, RoxDB leverages gRPC for efficient and high-performance communication.

This project is primarily focused on:

1. **DynamoDB-like API Implementation:** Implementing core DynamoDB-like functionalities using RocksDB for persistent
   storage.
2. **gRPC Integration:** Utilizing gRPC for communication, offering advantages like binary serialization, strong typing,
   and improved performance compared to HTTP JSON.
3. **Code Generation with Amazon Q:** Leveraging Amazon Q for code generation to accelerate development and ensure
   consistency.
4. **Consultation with Google Gemini:** Leveraging Google Gemini for consultations on software engineering best
   practices.

## Motivation

This project was born out of the desire to explore alternative storage solutions for DynamoDB-like workloads and to
experiment with the benefits of gRPC in data-intensive applications. It also serves as a practical demonstration of
using AI-powered code generation tools like Amazon Q to streamline development.

## Features

* **DynamoDB-like API:** `PutItem`, `UpdateItem`, `DeleteItem`, `GetItem`, `Query`, `TransactWriteItems`.
* **RocksDB Storage:** Utilizing RocksDB for fast and reliable data storage.
* **gRPC Interface:** Providing a high-performance gRPC API for client interactions.
* **Transport Security:** Support for TLS encryption and mutual TLS (mTLS) authentication for secure client-server
  communication.
* **Efficient Data Serialization:** Leveraging gRPC's Protocol Buffers for efficient data serialization.
* **Containerization & Orchestration:** multi-arch (`linux/amd64` and `linux/arm64`) Docker container support for easy
  deployment, quick-start Kubernetes manifests provided
* **Observability with OpenTelemetry:** Built-in OpenTelemetry integration for monitoring RocksDB metrics and
  operational insights.

## Getting Started

### Running the Server

1. **Pull and start the Docker container:**

   ```bash
   docker pull ghcr.io/lukaszbudnik/roxdb:edge
   docker run -d -p 50051:50051 ghcr.io/lukaszbudnik/roxdb:edge
   ```

2. **Or deploy RoxDB service to Kubernetes:**

   See [kubernetes/README.md](kubernetes/README.md).

4. **Test:**

   Using [grpcurl](https://github.com/fullstorydev/grpcurl):

   ```bash
   export ROXDB_ENDPOINT=localhost:50051
   # Describe the service
   grpcurl -plaintext ${ROXDB_ENDPOINT} describe com.github.lukaszbudnik.roxdb.v1.RoxDB
   # Check service health
   grpcurl -plaintext -d '{"service": "com.github.lukaszbudnik.roxdb.v1.RoxDB"}' ${ROXDB_ENDPOINT} grpc.health.v1.Health/Check
   # Stream PutItem, UpdateItem, GetItem, DeleteItem, Query, and TransactWriteItems in a single call
   grpcurl -d @ -plaintext ${ROXDB_ENDPOINT} com.github.lukaszbudnik.roxdb.v1.RoxDB/ProcessItems << EOM
   {
     "correlation_id": "123",
     "put_item": {
       "table": "users",
       "item": {
         "key": {
           "partition_key": "user#123",
           "sort_key": "settings"
         },
         "attributes": {
           "field1": "value1",
           "field2": 123
         }
       }
     }
   }
   {
     "correlation_id": "123",
     "put_item": {
       "table": "users",
       "item": {
         "key": {
           "partition_key": "user#123",
           "sort_key": "address"
         },
         "attributes": {
           "country": "Poland",
           "city": "Wejherowo"
         }
       }
     }
   }
   {
     "correlation_id": "124",
     "update_item": {
       "table": "users",
       "item": {
         "key": {
           "partition_key": "user#123",
           "sort_key": "settings"
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
     "get_item": {
       "table": "users",
       "key": {
         "partition_key": "user#123",
         "sort_key": "settings"
       }
     }
   }
   {
     "correlation_id": "query-with-no-sort-key-range-1",
     "query": {
       "table": "users",
       "partition_key": "user#123",
       "limit": 10
     }
   }
   {
     "correlation_id": "query-with-sort-key-and-boundaries-1",
     "query": {
       "table": "users",
       "partition_key": "user#123",
       "limit": 10,
       "sort_key_range": {
         "start": {
           "value": "address",
           "type": "INCLUSIVE"
         },
         "end": {
           "value": "settings",
           "type": "EXCLUSIVE"
         }
       }
     }
   }
   {
     "correlation_id": "126",
     "delete_item": {
       "table": "users",
       "key": {
         "partition_key": "user#123",
         "sort_key": "settings"
       }
     }
   }
   {
     "correlation_id": "127",
     "delete_item": {
       "table": "users",
       "key": {
         "partition_key": "user#123",
         "sort_key": "address"
       }
     }
   }
   {
     "correlation_id": "128",
     "get_item": {
       "table": "users",
       "key": {
         "partition_key": "user#123",
         "sort_key": "settings"
       }
     }
   }
   {
     "correlation_id": "txn-123",
     "transact_write_items": {
       "items": [
         {
           "put": {
             "table": "accounts",
             "item": {
             "key": {
               "partition_key": "user#1",
               "sort_key": "account"
             },
             "attributes": {
               "value": 30
             }
           }
          }
         },
         {
           "put": {
             "table": "accounts",
             "item": {
               "key": {
                 "partition_key": "user#2",
                 "sort_key": "account"
               },
               "attributes": {
                 "value": 70
               }
             }
           }
         }
       ]
     }
   }
   EOM
   ```

## Configuration

RoxDB can be configured using the following environment variables:

| Environment Variable               | Description                                                                                   | Required | Default      |
|------------------------------------|-----------------------------------------------------------------------------------------------|----------|--------------|
| `ROXDB_PORT`                       | The port number on which the gRPC server listens for incoming connections.                    | No       | 50501        |
| `ROXDB_DB_PATH`                    | File system path where RocksDB will store its data files.                                     | No       | /tmp/rocksdb |
| `ROXDB_TLS_PRIVATE_KEY_PATH`       | Path to the TLS private key file for secure communications. Required when TLS is enabled.     | No*      |              |
| `ROXDB_TLS_CERTIFICATE_PATH`       | Path to the TLS certificate file. Required when TLS is enabled.                               | No*      |              |
| `ROXDB_TLS_CERTIFICATE_CHAIN_PATH` | Path to the certificate chain file for TLS validation. Required when using mutual TLS (mTLS). | No**     |              |
| `ROXDB_OPENTELEMETRY_CONFIG`       | Path to OpenTelemetry configuration file for metrics collection and export.                   | No       |              |

\* `ROXDB_TLS_PRIVATE_KEY_PATH` and `ROXDB_TLS_CERTIFICATE_PATH` variables are required when running with TLS enabled.

\*\* `ROXDB_TLS_CERTIFICATE_CHAIN_PATH` is only required when running mutual authentication using mTLS.

### Example Configuration

```bash
# Basic setup
export ROXDB_PORT=8080
export ROXDB_DB_PATH=/data/roxdb

# TLS configuration
export ROXDB_TLS_PRIVATE_KEY_PATH=/etc/roxdb/tls/private.key
export ROXDB_TLS_CERTIFICATE_PATH=/etc/roxdb/tls/certificate.crt
export ROXDB_TLS_CERTIFICATE_CHAIN_PATH=/etc/roxdb/tls/chain.crt

# Observability
export ROXDB_OPENTELEMETRY_CONFIG=/etc/roxdb/otel-config.yaml
```

When `ROXDB_OPENTELEMETRY_CONFIG` is set, it must point to a valid yaml file:

```yaml
# gRPC OTLP endpoint for sending metrics
otlpEndpoint: http://localhost:4317
# interval at which collect metrics, in seconds
interval: 1
# list of org.rocksdb.TickerType or "*" for all tickers (beware RocksDB has 200+ tickers)
tickers:
  - "NUMBER_KEYS_WRITTEN"
  - "NUMBER_KEYS_READ"
  - "BYTES_WRITTEN"
  - "BYTES_READ"
  - "NO_FILE_OPENS"
  - "NO_FILE_ERRORS"
  - "STALL_MICROS"
# list of org.rocksdb.HistogramType or "*" for all histograms (beware RocksDB has 60+ histograms)
histograms:
  - "DB_GET"
  - "DB_WRITE"
  - "COMPACTION_TIME"
  - "COMPACTION_CPU_TIME"
  - "WAL_FILE_SYNC_MICROS"
  - "MANIFEST_FILE_SYNC_MICROS"
```

## Building the project locally

### Prerequisites

* Java 21 or later
* Gradle
* Protocol Buffer Compiler (`protoc`)
* Docker engine

### Installation

1. **Clone the repository:**

   ```bash
   git clone git@github.com:lukaszbudnik/roxdb.git
   cd roxdb
   ```

2. **Build the project:**

   ```bash
   ./gradlew build
   ```

3. **Start the RoxDB Java app:**

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

4. **Or build and start the RoxDB local container:**

   ```bash
   # Build the Docker image and pass ROXDB_VERSION build argument
   docker build --build-arg ROXDB_VERSION=1.0-SNAPSHOT -t roxdb .
   # Run with default db path
   docker run -P roxdb
   # Run with custom db path/volume
   docker run -P -e ROXDB_DB_PATH=/data/roxdb roxdb
   ```

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Format the code to comply with Google Java Style.
4. Commit your changes.
5. Push to your branch.
6. Submit a pull request.

## License

This project is licensed under the Apache 2.0 License. See the `LICENSE` file for details.

## Acknowledgments

* [RocksDB](https://rocksdb.org/)
* [gRPC](https://grpc.io/)
* [Amazon Q](https://aws.amazon.com/amazon-q/)
* [Google Gemini](https://gemini.google.com)

## Future Work

* Future features or improvements: delete by partition, global secondary indexes.
* Improve performance and scalability.
