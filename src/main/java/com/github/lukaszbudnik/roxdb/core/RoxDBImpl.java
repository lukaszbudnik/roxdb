package com.github.lukaszbudnik.roxdb.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class RoxDBImpl implements RoxDB {
    static {
        RocksDB.loadLibrary();
    }

    private final String dbPath;
    private final RocksDB db;
    private final Map<String, ColumnFamilyHandle> columnFamilies;
    private final ObjectMapper objectMapper;
    private final DBOptions dbOptions;
    private final List<ColumnFamilyHandle> columnFamilyHandles;

    private static final Logger log = LoggerFactory.getLogger(RoxDBImpl.class);

    public RoxDBImpl(String dbPath) throws RocksDBException {
        this.dbPath = dbPath;

        // Initialize column families
        this.columnFamilies = new HashMap<>();
        this.columnFamilyHandles = new ArrayList<>();

        Statistics statistics = new Statistics();
        // Create DB options
        this.dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true).setStatistics(statistics);

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

        this.objectMapper = new ObjectMapper(new MessagePackFactory());
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
        try {
            value = objectMapper.writeValueAsBytes(item.attributes());
        } catch (JsonProcessingException e) {
            log.error("Error serializing item: {}", item.key().toStorageKey(), e);
            throw new RuntimeException(e);
        }

        // Store in RocksDB
        db.put(cfHandle, key, value);

        log.debug("Item written: {}", item.key().toStorageKey());
    }

    // GetItem operation
    @Override
    public Item getItem(String tableName, Key key) throws RocksDBException {
        ColumnFamilyHandle cfHandle = getOrCreateColumnFamily(tableName);

        // Get from RocksDB
        byte[] value = db.get(cfHandle, key.toStorageKey().getBytes());

        if (value == null) {
            log.debug("Item not found: {}", key.toStorageKey());
            return null;
        }

        // Convert bytes to Map
        Map<String, Object> attributes = null;
        try {
            attributes = objectMapper.readValue(value, Map.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Item item = new Item(key, attributes);
        log.debug("Item found: {}", item.key().toStorageKey());
        return item;
    }

    // Query operation
    @Override
    public List<Item> query(String tableName, String partitionKey, Optional<String> sortKeyStart, Optional<String> sortKeyEnd) throws RocksDBException {
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
                try {
                    attributes = objectMapper.readValue(iterator.value(), Map.class);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                results.add(new Item(new Key(partitionKey, currentSortKey), attributes));

                iterator.next();
            }
        }

        log.debug("QueryResults for: {}#{}-{} found items: {}", partitionKey, sortKeyStart, sortKeyEnd, results.size());

        return results;
    }

    @Override
    public void deleteItem(String tableName, Key key) throws RocksDBException {
        ColumnFamilyHandle cfHandle = getOrCreateColumnFamily(tableName);
        db.delete(cfHandle, key.toStorageKey().getBytes());
        log.debug("Deleted: {}", key.toStorageKey());
    }

    @Override
    public void close() {
        // Close all column family handles
        for (ColumnFamilyHandle handle : columnFamilyHandles) {
            handle.close();
        }
        // Close the database
        db.close();
        // Close DB options
        dbOptions.close();
    }

}

