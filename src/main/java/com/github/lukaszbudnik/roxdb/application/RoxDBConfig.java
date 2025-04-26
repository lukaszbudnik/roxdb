package com.github.lukaszbudnik.roxdb.application;

public record RoxDBConfig(
    int port,
    String dbPath,
    String tlsCertificatePath,
    String tlsPrivateKeyPath,
    String tlsCertificateChainPath) {}
