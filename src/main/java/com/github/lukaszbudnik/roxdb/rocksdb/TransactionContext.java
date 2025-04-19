package com.github.lukaszbudnik.roxdb.rocksdb;

import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.Transaction;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class TransactionContext {
  private final Logger logger = org.slf4j.LoggerFactory.getLogger(TransactionContext.class);
  private final Transaction transaction;
  private final RoxDB roxDB;

  public TransactionContext(RoxDB roxDB, Transaction transaction) {
    this.transaction = transaction;
    this.roxDB = roxDB;
  }

  public void put(String tableName, Item item) throws RocksDBException {
    byte[] key = SerDeUtils.serializeKey(item);
    byte[] value = SerDeUtils.serializeAttributes(item);
    transaction.put(roxDB.getOrCreateColumnFamily(tableName), key, value);
    logger.debug("Transaction {} put: {}", transaction.getID(), item.key().toStorageKey());
  }

  public void update(String tableName, Item item) throws RocksDBException {
    Item existingItem = get(tableName, item.key());

    if (existingItem == null) {
      // If item doesn't exist, perform a put operation
      put(tableName, item);
      return;
    }

    // Merge existing and new attributes
    Map<String, Object> mergedAttributes = new HashMap<>(existingItem.attributes());
    mergedAttributes.putAll(item.attributes());

    // Create new item with merged attributes
    Item updatedItem = new Item(item.key(), mergedAttributes);

    put(tableName, updatedItem);
  }

  public void delete(String tableName, Key key) throws RocksDBException {
    byte[] keyBytes = key.toStorageKey().getBytes();
    transaction.delete(roxDB.getOrCreateColumnFamily(tableName), keyBytes);
    logger.debug("Transaction {} delete: {}", transaction.getID(), key.toStorageKey());
  }

  // get operation
  public Item get(String tableName, Key key) throws RocksDBException {
    byte[] keyBytes = key.toStorageKey().getBytes();
    byte[] value =
        transaction.get(new ReadOptions(), roxDB.getOrCreateColumnFamily(tableName), keyBytes);

    if (value == null) {
      logger.debug("Transaction {} item not found: {}", transaction.getID(), key.toStorageKey());
      return null;
    }

    // Convert bytes to Map
    Map<String, Object> attributes = SerDeUtils.deserializeAttributes(value);
    Item item = new Item(key, attributes);
    logger.debug("Transaction {} item found: {}", transaction.getID(), item.key().toStorageKey());
    return item;
  }
}
