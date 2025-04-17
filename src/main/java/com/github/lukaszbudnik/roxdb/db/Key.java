package com.github.lukaszbudnik.roxdb.db;

public record Key(String partitionKey, String sortKey) {
  public String toStorageKey() {
    return partitionKey + "#" + sortKey;
  }
}
