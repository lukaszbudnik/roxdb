package com.github.lukaszbudnik.roxdb.rocksdb;

public record Key(String partitionKey, String sortKey) {}
