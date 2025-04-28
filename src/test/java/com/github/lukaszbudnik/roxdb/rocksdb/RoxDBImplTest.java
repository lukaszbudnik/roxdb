package com.github.lukaszbudnik.roxdb.rocksdb;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;

class RoxDBImplTest {

  @TempDir Path dbPath = null;
  RoxDBImpl roxdb = null;

  @BeforeEach
  void setUp() throws RocksDBException {
    roxdb = new RoxDBImpl(dbPath.toString());
  }

  @AfterEach
  void tearDown() {
    roxdb.close();
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
    Assertions.assertEquals(item, retrievedItem);

    // Update item
    Map<String, Object> updatedAttributes = new HashMap<>();
    updatedAttributes.put("new_attribute", "new value");
    updatedAttributes.put("message", "Hello World!");
    Item updatedItem = new Item(key, updatedAttributes);
    roxdb.updateItem("users", updatedItem);

    // Get updated item
    Item retrievedUpdatedItem = roxdb.getItem("users", key);
    Assertions.assertEquals(updatedItem.key(), retrievedUpdatedItem.key());
    // the item is updated and should have attributes from the original retrievedItem and
    // updatedItem merged
    // the size of the attributes map should be 3 and the attributes map should have both entries
    Assertions.assertEquals(3, retrievedUpdatedItem.attributes().size());
    Assertions.assertTrue(
        retrievedUpdatedItem
            .attributes()
            .keySet()
            .containsAll(retrievedItem.attributes().keySet()));
    Assertions.assertTrue(
        retrievedUpdatedItem.attributes().keySet().containsAll(updatedItem.attributes().keySet()));

    // Delete item
    roxdb.deleteItem("users", key);
    Item retrievedDeletedItem = roxdb.getItem("users", key);
    Assertions.assertNull(retrievedDeletedItem);

    // Put item
    Key key2 = new Key("user123", "payment");
    Map<String, Object> attributes2 = new HashMap<>();
    attributes2.put("type", "cash");
    Item item2 = new Item(key2, attributes2);
    // Update on non-existing item will create that item
    roxdb.updateItem("users", item2);

    Item retrievedItem2 = roxdb.getItem("users", key2);
    Assertions.assertEquals(item2, retrievedItem2);
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

    Map<String, Object> settings = new HashMap<>();
    settings.put("theme", "dark");
    Key settingsKey = new Key("user123", "settings");
    Item settingsItem = new Item(settingsKey, settings);
    roxdb.putItem("users", settingsItem);

    Map<String, Object> profileUser2 = new HashMap<>();
    profileUser2.put("message", "");
    Key profileUser2Key = new Key("user124", "profile");
    Item otherUserItem = new Item(profileUser2Key, profileUser2);
    roxdb.putItem("users", otherUserItem);

    // Query all items
    List<Item> queryResults =
        roxdb.query(
            "users", // table name
            "user123", // partition key
            10, // limit
            Optional.empty() // empty sort key range
            );
    Assertions.assertEquals(4, queryResults.size());
    // keys are sorted so first is address, payment, then profile
    Assertions.assertEquals(addressItem, queryResults.get(0));
    Assertions.assertEquals(paymentItem, queryResults.get(1));
    Assertions.assertEquals(profileItem, queryResults.get(2));
    Assertions.assertEquals(settingsItem, queryResults.get(3));

    // Query items using sort key start from "p" inclusive
    // for sort key start the inclusive option behaves like startsWith
    // this will include payment, profile, and settings and will exclude address
    List<Item> queryResultsPrefixP =
        roxdb.query(
            "users", // table name
            "user123", // partition key
            10, // limit
            Optional.of(SortKeyRange.from(RangeBoundary.inclusive("p"))));
    Assertions.assertEquals(3, queryResultsPrefixP.size());
    Assertions.assertEquals(paymentItem, queryResultsPrefixP.get(0));
    Assertions.assertEquals(profileItem, queryResultsPrefixP.get(1));
    Assertions.assertEquals(settingsItem, queryResultsPrefixP.get(2));

    // Query items using sort key start up to "pro"
    // for sort key end the inclusive option behaves like a full match
    // this will include address and payment, but will exclude "profile"
    List<Item> queryResultsPrefixUpToPro =
        roxdb.query(
            "users", // table name
            "user123", // partition key
            10, // limit
            Optional.of(SortKeyRange.to(RangeBoundary.inclusive("pro"))));
    Assertions.assertEquals(2, queryResultsPrefixUpToPro.size());
    Assertions.assertEquals(addressItem, queryResultsPrefixUpToPro.get(0));
    Assertions.assertEquals(paymentItem, queryResultsPrefixUpToPro.get(1));

    // Query items using sort key start between "a" and "s"
    // for sort key start the inclusive option behaves like startsWith
    // for sort key end the inclusive option behaves like a full match
    // this will include address, payment, profile, but will exclude "settings"
    List<Item> queryResultsPrefixAtoS =
        roxdb.query(
            "users", // table name
            "user123", // partition key
            10, // limit
            Optional.of(
                SortKeyRange.between(RangeBoundary.inclusive("a"), RangeBoundary.inclusive("s"))));
    Assertions.assertEquals(3, queryResultsPrefixAtoS.size());
    Assertions.assertEquals(addressItem, queryResultsPrefixAtoS.get(0));
    Assertions.assertEquals(paymentItem, queryResultsPrefixAtoS.get(1));
    Assertions.assertEquals(profileItem, queryResultsPrefixAtoS.get(2));
  }

  @Test
  void queryPagination() throws RocksDBException {
    for (int i = 0; i < 100; i++) {
      Map<String, Object> profile = new HashMap<>();
      profile.put("message", "Hello World");
      // pad profile number with leading zeros so that they are sorted correctly by RocksDB (RocksDB
      // sorts keys lexicographically) e.g. profile01, profile02, profile03, etc.
      Key profileKey = new Key("user123", String.format("profile%02d", i));
      Item profileItem = new Item(profileKey, profile);
      roxdb.putItem("users", profileItem);
    }

    // first call
    List<Item> queryResults =
        roxdb.query(
            "users", // table name
            "user123", // partition key
            10, // limit
            Optional.empty() // empty sort key range
            );
    Assertions.assertEquals(10, queryResults.size());
    for (int i = 0; i < 10; i++) {
      Assertions.assertEquals(String.format("profile%02d", i), queryResults.get(i).key().sortKey());
    }

    // next call
    String lastEvaluatedKey = queryResults.get(9).key().sortKey();
    List<Item> queryResultsNext =
        roxdb.query(
            "users", // table name
            "user123", // partition key
            10, // limit
            Optional.of(
                SortKeyRange.from(RangeBoundary.exclusive(lastEvaluatedKey))) // exclusive start key
            );
    Assertions.assertEquals(10, queryResultsNext.size());
    for (int i = 0; i < 10; i++) {
      Assertions.assertEquals("profile" + (i + 10), queryResultsNext.get(i).key().sortKey());
    }
  }

  @Test
  void queryBetweenInclusiveInclusive() throws RocksDBException {
    for (int i = 0; i < 10; i++) {
      Map<String, Object> profile = new HashMap<>();
      profile.put("message", "Hello World");
      // pad profile number with leading zeros so that they are sorted correctly by RocksDB (RocksDB
      // sorts keys lexicographically) e.g. profile01, profile02, profile03, etc.
      Key profileKey = new Key("user123", String.format("profile%02d", i));
      Item profileItem = new Item(profileKey, profile);
      roxdb.putItem("users", profileItem);
    }

    // first call
    List<Item> queryResults =
        roxdb.query(
            "users", // table name
            "user123", // partition key
            10, // limit
            Optional.of(
                SortKeyRange.between(
                    RangeBoundary.inclusive("profile02"), RangeBoundary.inclusive("profile07"))));
    Assertions.assertEquals(6, queryResults.size());
    for (int i = 2; i <= 7; i++) {
      Assertions.assertEquals(
          String.format("profile%02d", i), queryResults.get(i - 2).key().sortKey());
    }
  }

  @Test
  void queryBetweenInclusiveExclusive() throws RocksDBException {
    for (int i = 0; i < 10; i++) {
      Map<String, Object> profile = new HashMap<>();
      profile.put("message", "Hello World");
      // pad profile number with leading zeros so that they are sorted correctly by RocksDB (RocksDB
      // sorts keys lexicographically) e.g. profile01, profile02, profile03, etc.
      Key profileKey = new Key("user123", String.format("profile%02d", i));
      Item profileItem = new Item(profileKey, profile);
      roxdb.putItem("users", profileItem);
    }

    // first call
    List<Item> queryResults =
        roxdb.query(
            "users", // table name
            "user123", // partition key
            10, // limit
            Optional.of(
                SortKeyRange.between(
                    RangeBoundary.inclusive("profile02"), RangeBoundary.exclusive("profile07"))));
    Assertions.assertEquals(5, queryResults.size());
    for (int i = 2; i < 7; i++) {
      Assertions.assertEquals(
          String.format("profile%02d", i), queryResults.get(i - 2).key().sortKey());
    }
  }

  @Test
  void queryBetweenExclusiveInclusive() throws RocksDBException {
    for (int i = 0; i < 10; i++) {
      Map<String, Object> profile = new HashMap<>();
      profile.put("message", "Hello World");
      // pad profile number with leading zeros so that they are sorted correctly by RocksDB (RocksDB
      // sorts keys lexicographically) e.g. profile01, profile02, profile03, etc.
      Key profileKey = new Key("user123", String.format("profile%02d", i));
      Item profileItem = new Item(profileKey, profile);
      roxdb.putItem("users", profileItem);
    }

    // first call
    List<Item> queryResults =
        roxdb.query(
            "users", // table name
            "user123", // partition key
            10, // limit
            Optional.of(
                SortKeyRange.between(
                    RangeBoundary.exclusive("profile02"), RangeBoundary.inclusive("profile07"))));
    Assertions.assertEquals(5, queryResults.size());
    for (int i = 2 + 1; i <= 7; i++) {
      Assertions.assertEquals(
          String.format("profile%02d", i), queryResults.get(i - 3).key().sortKey());
    }
  }

  @Test
  void queryBetweenExclusiveExclusive() throws RocksDBException {
    for (int i = 0; i < 10; i++) {
      Map<String, Object> profile = new HashMap<>();
      profile.put("message", "Hello World");
      // pad profile number with leading zeros so that they are sorted correctly by RocksDB (RocksDB
      // sorts keys lexicographically) e.g. profile01, profile02, profile03, etc.
      Key profileKey = new Key("user123", String.format("profile%02d", i));
      Item profileItem = new Item(profileKey, profile);
      roxdb.putItem("users", profileItem);
    }

    // first call
    List<Item> queryResults =
        roxdb.query(
            "users", // table name
            "user123", // partition key
            10, // limit
            Optional.of(
                SortKeyRange.between(
                    RangeBoundary.exclusive("profile02"), RangeBoundary.exclusive("profile07"))));
    Assertions.assertEquals(4, queryResults.size());
    for (int i = 2 + 1; i < 7; i++) {
      Assertions.assertEquals(
          String.format("profile%02d", i), queryResults.get(i - 3).key().sortKey());
    }
  }

  @Test
  void transaction() throws RocksDBException {
    Key key = new Key("user123", "profile");
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("message", "Hello World");
    attributes.put("number", 123);
    Item item = new Item(key, attributes);

    Key key2 = new Key("user123", "payment");
    Map<String, Object> attributes2 = new HashMap<>();
    attributes2.put("payment", "12345");
    Item item2 = new Item(key2, attributes2);

    String table = "users";

    roxdb.executeTransaction(
        (txCtx) -> {
          txCtx.put(table, item);
          // item2 does not exist - update will call put straight away
          txCtx.update(table, item2);
          // item already exists - update will merge new and old attributes
          item.attributes().put("new_attribute", "new value");
          txCtx.update(table, item);
          txCtx.delete(table, key2);
        });

    Item retrievedItem = roxdb.getItem("users", key);
    Assertions.assertEquals(item, retrievedItem);

    Item retrievedItem2 = roxdb.getItem("users", key2);
    Assertions.assertNull(retrievedItem2);
  }

  // test interrupted transaction
  @Test
  void transactionInterrupted() throws RocksDBException {
    Key key = new Key("user123", "profile");
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("message", "Hello World");
    attributes.put("number", 123);
    Item item = new Item(key, attributes);

    Key key2 = new Key("user123", "payment");
    Map<String, Object> attributes2 = new HashMap<>();
    attributes2.put("payment", "12345");
    Item item2 = new Item(key2, attributes2);

    String table = "users";

    Assertions.assertThrows(
        RuntimeException.class,
        () -> {
          roxdb.executeTransaction(
              (txCtx) -> {
                txCtx.put(table, item);
                // item2 does not exist - update will call put straight away
                txCtx.update(table, item2);
                // item already exists - update will merge new and old attributes
                item.attributes().put("new_attribute", "new value");
                txCtx.update(table, item);
                txCtx.delete(table, key2);
                throw new RuntimeException("interrupted");
              });
        });
    Item retrievedItem = roxdb.getItem("users", key);
    Assertions.assertNull(retrievedItem);
    Item retrievedItem2 = roxdb.getItem("users", key2);
    Assertions.assertNull(retrievedItem2);
  }
}
