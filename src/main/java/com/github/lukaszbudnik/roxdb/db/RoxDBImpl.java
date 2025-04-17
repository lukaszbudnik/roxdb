package com.github.lukaszbudnik.roxdb.db;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.lukaszbudnik.roxdb.api.Item;
import com.github.lukaszbudnik.roxdb.api.Key;
import com.github.lukaszbudnik.roxdb.api.RoxDB;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RoxDBImpl implements RoxDB {
  private static final Logger logger = LoggerFactory.getLogger(RoxDBImpl.class);

  static {
    RocksDB.loadLibrary();
  }

  private final String dbPath;
  private final RocksDB db;
  private final Map<String, ColumnFamilyHandle> columnFamilies;
  private final Kryo kryo;
  private final DBOptions dbOptions;
  private final List<ColumnFamilyHandle> columnFamilyHandles;

  public RoxDBImpl(String dbPath) throws RocksDBException {
    logger.info("Initializing RocksDB instance at {}", dbPath);

    this.dbPath = dbPath;

    // Initialize column families
    this.columnFamilies = new HashMap<>();
    this.columnFamilyHandles = new ArrayList<>();

    Statistics statistics = new Statistics();
    // Create DB options
    this.dbOptions =
        new DBOptions()
            .setCreateIfMissing(true)
            .setCreateMissingColumnFamilies(true)
            .setStatistics(statistics);

    // Get list of existing column families
    List<byte[]> existingCFs = RocksDB.listColumnFamilies(new Options(), dbPath);

    // Prepare column family descriptors
    List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>();
    // Always add default column family
    columnFamilyDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));

    // Add existing column families
    for (byte[] cf : existingCFs) {
      if (!Arrays.equals(cf, RocksDB.DEFAULT_COLUMN_FAMILY)) {
        columnFamilyDescriptors.add(new ColumnFamilyDescriptor(cf));
      }
    }

    // Open DB with column families
    this.db = RocksDB.open(dbOptions, dbPath, columnFamilyDescriptors, columnFamilyHandles);

    // Map column family handles
    for (int i = 0; i < columnFamilyHandles.size(); i++) {
      String cfName = new String(columnFamilyDescriptors.get(i).getName());
      columnFamilies.put(cfName, columnFamilyHandles.get(i));
    }

    this.kryo = new Kryo();
    this.kryo.register(HashMap.class);

    logger.info("RocksDB instance initialized");
  }

  private ColumnFamilyHandle getOrCreateColumnFamily(String tableName) throws RocksDBException {
    if (!columnFamilies.containsKey(tableName)) {
      ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(tableName.getBytes());
      ColumnFamilyHandle cfHandle = db.createColumnFamily(cfDescriptor);
      columnFamilies.put(tableName, cfHandle);
      columnFamilyHandles.add(cfHandle);
      return cfHandle;
    }
    return columnFamilies.get(tableName);
  }

  // PutItem operation
  @Override
  public void putItem(String tableName, Item item) throws RocksDBException {
    ColumnFamilyHandle cfHandle = getOrCreateColumnFamily(tableName);

    // Convert key to bytes
    byte[] key = item.key().toStorageKey().getBytes();
    // Convert attributes to bytes
    byte[] value = null;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos)) {
      kryo.writeObject(output, item.attributes());
      // needs explicit flush
      output.flush();
      value = baos.toByteArray();
    } catch (IOException e) {
      logger.error("Error serializing item: {}", item.key().toStorageKey(), e);
      throw new RuntimeException(e);
    }

    // Store in RocksDB
    db.put(cfHandle, key, value);

    logger.debug("Item put: {}", item.key().toStorageKey());
  }

  //  UpdateItem operation
  @Override
  public void updateItem(String tableName, Item item) throws RocksDBException {
    Item existingItem = getItem(tableName, item.key());

    if (existingItem == null) {
      // If item doesn't exist, perform a put operation
      putItem(tableName, item);
      return;
    }

    // Merge existing and new attributes
    Map<String, Object> mergedAttributes = new HashMap<>(existingItem.attributes());
    mergedAttributes.putAll(item.attributes());

    // Create new item with merged attributes
    Item updatedItem = new Item(item.key(), mergedAttributes);

    putItem(tableName, updatedItem);
  }

  // GetItem operation
  @Override
  public Item getItem(String tableName, Key key) throws RocksDBException {
    ColumnFamilyHandle cfHandle = getOrCreateColumnFamily(tableName);

    // Get from RocksDB
    byte[] value = db.get(cfHandle, key.toStorageKey().getBytes());

    if (value == null) {
      logger.debug("Item not found: {}", key.toStorageKey());
      return null;
    }

    // Convert bytes to Map
    Map<String, Object> attributes = null;
    try (Input input = new Input(value)) {
      attributes = kryo.readObject(input, HashMap.class);
    }
    Item item = new Item(key, attributes);
    logger.debug("Item found: {}", item.key().toStorageKey());
    return item;
  }

  // Query operation
  @Override
  public List<Item> query(
      String tableName,
      String partitionKey,
      Optional<String> sortKeyStart,
      Optional<String> sortKeyEnd)
      throws RocksDBException {
    List<Item> results = new ArrayList<>();
    ColumnFamilyHandle cfHandle = getOrCreateColumnFamily(tableName);

    // Create RocksDB iterator
    try (RocksIterator iterator = db.newIterator(cfHandle)) {
      // Seek to the first potential match
      String seekKey = partitionKey + "#";
      iterator.seek(seekKey.getBytes());

      // Iterate through matching items
      while (iterator.isValid()) {
        String currentKey = new String(iterator.key());

        // Check if we're still in the same partition
        if (!currentKey.startsWith(seekKey)) {
          break;
        }

        String currentSortKey = currentKey.split("#")[1];

        // Apply sort key range conditions if specified
        if (sortKeyStart.isPresent() && currentSortKey.compareTo(sortKeyStart.get()) < 0) {
          iterator.next();
          continue;
        }

        if (sortKeyEnd.isPresent() && currentSortKey.compareTo(sortKeyEnd.get()) > 0) {
          break;
        }

        // Add matching item to results
        Map<String, Object> attributes = null;
        try (Input input = new Input(iterator.value())) {
          attributes = kryo.readObject(input, HashMap.class);
        }

        results.add(new Item(new Key(partitionKey, currentSortKey), attributes));

        iterator.next();
      }
    }

    logger.debug(
        "QueryResults for: {}#{}-{} found items: {}",
        partitionKey,
        sortKeyStart,
        sortKeyEnd,
        results.size());

    return results;
  }

  @Override
  public void deleteItem(String tableName, Key key) throws RocksDBException {
    ColumnFamilyHandle cfHandle = getOrCreateColumnFamily(tableName);
    db.delete(cfHandle, key.toStorageKey().getBytes());
    logger.debug("Deleted: {}", key.toStorageKey());
  }

  @Override
  public void close() {
    logger.info("Closing RocksDB instance");
    // Close all column family handles
    for (ColumnFamilyHandle handle : columnFamilyHandles) {
      handle.close();
    }
    // Close the database
    db.close();
    // Close DB options
    dbOptions.close();
    logger.info("RocksDB instance closed successfully");
  }
}
