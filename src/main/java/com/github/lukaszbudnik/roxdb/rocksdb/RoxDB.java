package com.github.lukaszbudnik.roxdb.rocksdb;

import java.util.List;
import java.util.Optional;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

public interface RoxDB extends AutoCloseable {
  ColumnFamilyHandle getOrCreateColumnFamily(String tableName) throws RocksDBException;

  void putItem(String tableName, Item item) throws RocksDBException;

  void updateItem(String tableName, Item item) throws RocksDBException;

  Item getItem(String tableName, Key key) throws RocksDBException;

  List<Item> query(
      String tableName, String partitionKey, int limit, Optional<SortKeyRange> sortKeyRange)
      throws RocksDBException;

  void deleteItem(String tableName, Key key) throws RocksDBException;

  void executeTransaction(TransactionOperations transactionContext) throws RocksDBException;
}
