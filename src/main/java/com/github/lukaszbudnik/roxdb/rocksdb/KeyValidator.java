package com.github.lukaszbudnik.roxdb.rocksdb;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import static com.github.lukaszbudnik.roxdb.rocksdb.RoxDBImpl.PARTITION_SORT_KEY_SEPARATOR;

public class KeyValidator {
  public static List<ValidationResult> isValid(Key key) {
    List<ValidationResult> validationResults = new ArrayList<>();
    if (key.partitionKey().isEmpty()) {
      validationResults.add(new ValidationResult(false, "Partition key cannot be blank"));
    }
    if (key.partitionKey().contains(String.valueOf(PARTITION_SORT_KEY_SEPARATOR))) {
      validationResults.add(
          new ValidationResult(
              false,
              String.format(
                  "Partition key cannot contain character U+%04X",
                  (int) PARTITION_SORT_KEY_SEPARATOR)));
    }
    if (key.sortKey().isEmpty()) {
      validationResults.add(new ValidationResult(false, "Sort key cannot be blank"));
    }
    if (key.sortKey().contains(String.valueOf(PARTITION_SORT_KEY_SEPARATOR))) {
      validationResults.add(
          new ValidationResult(
              false,
              String.format(
                  "Sort key cannot contain character U+%04X", (int) PARTITION_SORT_KEY_SEPARATOR)));
    }
    return validationResults;
  }
}
