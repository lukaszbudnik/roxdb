package com.github.lukaszbudnik.roxdb.api;

public record Key(String partitionKey, String sortKey) {
    public String toStorageKey() {
        return partitionKey + "#" + sortKey;
    }
}