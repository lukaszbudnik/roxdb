# RoxDB

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![Java CI](https://github.com/lukaszbudnik/roxdb/actions/workflows/gradle.yml/badge.svg)](https://github.com/lukaszbudnik/roxdb/actions/workflows/gradle.yml) [![GitHub Issues](https://img.shields.io/github/issues/lukaszbudnik/roxdb)](https://github.com/lukaszbudnik/roxdb/issues)

**DynamoDB implementation on RocksDB over gRPC.**

## Project Description

RoxDB is an open-source project that aims to provide a basic implementation of the Amazon DynamoDB API using RocksDB as the storage engine. Instead of the standard HTTP JSON API, RoxDB leverages gRPC for efficient and high-performance communication.

This project is primarily focused on:

1.  **Basic DynamoDB API Implementation:** Implementing core DynamoDB functionalities using RocksDB for persistent storage.
2.  **gRPC Integration:** Utilizing gRPC for communication, offering advantages like binary serialization, strong typing, and improved performance compared to HTTP JSON.
3.  **Code Generation with Amazon Q:** Leveraging Amazon Q for code generation to accelerate development and ensure consistency.
4.  **Consultation with Google Gemini:** Leveraging Google Gemini for consultations on software engineering best practices.

## Motivation

This project was born out of the desire to explore alternative storage solutions for DynamoDB-like workloads and to experiment with the benefits of gRPC in data-intensive applications. It also serves as a practical demonstration of using AI-powered code generation tools like Amazon Q to streamline development.

## Features

* **Core DynamoDB API:** `PutItem`, `DeleteItem`, `GetItem`, `Query`.
* **RocksDB Storage:** Utilizing RocksDB for fast and reliable data storage.
* **gRPC Interface:** Providing a high-performance gRPC API for client interactions.
* **Efficient Data Serialization:** Leveraging gRPC's Protocol Buffers for efficient data serialization.

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
    ./gradlew test
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
* Kubernetes deployment.
