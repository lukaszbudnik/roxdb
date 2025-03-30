package com.github.lukaszbudnik.roxdb.core;

public record Key(String partitionKey, String sortKey) {
    public String toStorageKey() {
        return partitionKey + "#" + sortKey;
    }
}