package com.github.lukaszbudnik.roxdb.core;

import org.rocksdb.RocksDBException;

import java.util.List;
import java.util.Optional;

public interface RoxDB extends AutoCloseable {
    // PutItem operation
    void putItem(String tableName, Item item) throws RocksDBException;

    // GetItem operation
    Item getItem(String tableName, Key key) throws RocksDBException;

    // Query operation
    List<Item> query(String tableName, String partitionKey, Optional<String> sortKeyStart, Optional<String> sortKeyEnd) throws RocksDBException;

    // Delete operation
    void deleteItem(String tableName, Key key) throws RocksDBException;

}
