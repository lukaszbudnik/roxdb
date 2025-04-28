package com.github.lukaszbudnik.roxdb.rocksdb;

public record RangeBoundary(String value, RangeType type) {
  public static RangeBoundary inclusive(String value) {
    return new RangeBoundary(value, RangeType.INCLUSIVE);
  }

  public static RangeBoundary exclusive(String value) {
    return new RangeBoundary(value, RangeType.EXCLUSIVE);
  }
}
