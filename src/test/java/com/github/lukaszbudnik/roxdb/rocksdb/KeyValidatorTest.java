package com.github.lukaszbudnik.roxdb.rocksdb;

import static com.github.lukaszbudnik.roxdb.rocksdb.RoxDBImpl.PARTITION_SORT_KEY_SEPARATOR;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class KeyValidatorTest {

  @Test
  void testValidKey() {
    // Given
    Key key = new Key("validPartition", "validSort");

    // When
    List<ValidationResult> results = KeyValidator.isValid(key);

    // Then
    assertTrue(results.isEmpty());
  }

  @Test
  void testBlankPartitionKey() {
    // Given
    Key key = new Key("", "validSort");

    // When
    List<ValidationResult> results = KeyValidator.isValid(key);

    // Then
    assertEquals(1, results.size());
    assertFalse(results.get(0).valid());
    assertEquals("Partition key cannot be blank", results.get(0).errorMessage());
  }

  @Test
  void testBlankSortKey() {
    // Given
    Key key = new Key("validPartition", "");

    // When
    List<ValidationResult> results = KeyValidator.isValid(key);

    // Then
    assertEquals(1, results.size());
    assertFalse(results.get(0).valid());
    assertEquals("Sort key cannot be blank", results.get(0).errorMessage());
  }

  @Test
  void testPartitionKeyWithSeparator() {
    // Given
    Key key = new Key("invalid" + PARTITION_SORT_KEY_SEPARATOR + "partition", "validSort");

    // When
    List<ValidationResult> results = KeyValidator.isValid(key);

    // Then
    assertEquals(1, results.size());
    assertFalse(results.get(0).valid());
    assertTrue(results.get(0).errorMessage().startsWith("Partition key cannot contain"));
  }

  @Test
  void testSortKeyWithSeparator() {
    // Given
    Key key = new Key("validPartition", "invalid" + PARTITION_SORT_KEY_SEPARATOR + "sort");

    // When
    List<ValidationResult> results = KeyValidator.isValid(key);

    // Then
    assertEquals(1, results.size());
    assertFalse(results.get(0).valid());
    assertTrue(results.get(0).errorMessage().startsWith("Sort key cannot contain"));
  }

  @Test
  void testMultipleValidationFailures() {
    // Given
    Key key = new Key("", "");

    // When
    List<ValidationResult> results = KeyValidator.isValid(key);

    // Then
    assertEquals(2, results.size());
    results.forEach(result -> assertFalse(result.valid()));
  }
}
