package com.github.lukaszbudnik.roxdb.db;

import com.github.lukaszbudnik.roxdb.api.Item;
import com.github.lukaszbudnik.roxdb.api.Key;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

class RoxDBImplTest {

    private static final Logger log = LoggerFactory.getLogger(RoxDBImplTest.class);
    private String dbPath = null;
    private RoxDBImpl roxdb = null;

    @BeforeEach
    void setUp() throws RocksDBException {
        // Clean up the database before each test
        dbPath = "/tmp/roxdb-test-" + System.currentTimeMillis();
        roxdb = new RoxDBImpl(dbPath);
    }

    @AfterEach
    void tearDown() {
//        System.out.println("=== Database-wide Metrics ===");
//        System.out.println(roxdb.getDatabaseMetrics());
//        System.out.println();
//
//        System.out.println("=== Column Family Metrics: users ===");
//        System.out.println(roxdb.getColumnFamilyMetrics("users"));
//        System.out.println();

        // destroy the database after each test
        roxdb.close();
        FileUtils.deleteQuietly(new File(dbPath));
    }

    @Test
    void crud() throws RocksDBException {
        // Put item
        Key key = new Key("user123", "profile");
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("message", "Hello World");
        attributes.put("number", 123);
        Item item = new Item(key, attributes);
        roxdb.putItem("users", item);

        // Get item
        Item retrievedItem = roxdb.getItem("users", key);
        log.trace("Retrieved item: {}", retrievedItem);
        Assertions.assertEquals(item, retrievedItem);

        // Update item
        Map<String, Object> updatedAttributes = new HashMap<>();
        updatedAttributes.put("new_attribute", "new value");
        updatedAttributes.put("message", "Hello World!");
        Item updatedItem = new Item(key, updatedAttributes);
        roxdb.updateItem("users", updatedItem);

        // Get updated item
        Item retrievedUpdatedItem = roxdb.getItem("users", key);
        log.trace("Retrieved item: {}", retrievedUpdatedItem);
        Assertions.assertEquals(updatedItem.key(), retrievedUpdatedItem.key());
        // the item is updated and should have attributes from the original retrievedItem and updatedItem merged
        // the size of the attributes map should be 3 and the attributes map should have both entries
        Assertions.assertEquals(3, retrievedUpdatedItem.attributes().size());
        Assertions.assertTrue(retrievedUpdatedItem.attributes().keySet().containsAll(retrievedItem.attributes().keySet()));
        Assertions.assertTrue(retrievedUpdatedItem.attributes().keySet().containsAll(updatedItem.attributes().keySet()));

        // Delete item
        roxdb.deleteItem("users", key);
        Item retrievedDeletedItem = roxdb.getItem("users", key);
        log.trace("Retrieved item: {}", retrievedDeletedItem);
        Assertions.assertNull(retrievedDeletedItem);
    }

    @Test
    void query() throws RocksDBException {
        Map<String, Object> profile = new HashMap<>();
        profile.put("message", "Hello World");
        Key profileKey = new Key("user123", "profile");
        Item profileItem = new Item(profileKey, profile);
        roxdb.putItem("users", profileItem);

        Map<String, Object> payment = new HashMap<>();
        payment.put("payment", "12345");
        Key paymentKey = new Key("user123", "payment");
        Item paymentItem = new Item(paymentKey, payment);
        roxdb.putItem("users", paymentItem);

        Map<String, Object> address = new HashMap<>();
        address.put("country", "Poland");
        Key addressKey = new Key("user123", "address");
        Item addressItem = new Item(addressKey, address);
        roxdb.putItem("users", addressItem);

        Map<String, Object> profileUser2 = new HashMap<>();
        profileUser2.put("message", "");
        Key profileUser2Key = new Key("user124", "profile");
        Item otherUserItem = new Item(profileUser2Key, profileUser2);
        roxdb.putItem("users", otherUserItem);

        // Query items
        List<Item> queryResults = roxdb.query(
                "users",      // table name
                "user123",        // partition key
                Optional.empty(), // sort key start
                Optional.empty()  // sort key end
        );
        log.trace("Query results: {}", queryResults);
        Assertions.assertEquals(3, queryResults.size());
        // keys are sorted so first is address, payment, then profile
        Assertions.assertEquals(addressItem, queryResults.get(0));
        Assertions.assertEquals(paymentItem, queryResults.get(1));
        Assertions.assertEquals(profileItem, queryResults.get(2));

        // Query items using sort key start p
        // this will include payment and profile
        // and will exclude address
        List<Item> queryResultsPrefixP = roxdb.query(
                "users",      // table name
                "user123",    // partition key
                Optional.of("p"), // sort key start
                Optional.empty()  // sort key end
        );
        log.trace("Query results: {}", queryResultsPrefixP);
        Assertions.assertEquals(2, queryResultsPrefixP.size());
        Assertions.assertEquals(paymentItem, queryResultsPrefixP.get(0));
        Assertions.assertEquals(profileItem, queryResultsPrefixP.get(1));

        // Query items using sort key start a and sort key end b
        // this will include only address
        List<Item> queryResultsPrefixAB = roxdb.query(
                "users",      // table name
                "user123",    // partition key
                Optional.of("a"), // sort key start
                Optional.of("b")  // sort key end
        );
        log.trace("Query results: {}", queryResultsPrefixAB);
        Assertions.assertEquals(1, queryResultsPrefixAB.size());
        Assertions.assertEquals(addressItem, queryResultsPrefixAB.get(0));
    }
}