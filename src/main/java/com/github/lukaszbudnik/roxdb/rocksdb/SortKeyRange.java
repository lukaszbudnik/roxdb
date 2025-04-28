package com.github.lukaszbudnik.roxdb.rocksdb;

import java.util.Optional;

public record SortKeyRange(Optional<RangeBoundary> start, Optional<RangeBoundary> end) {
  public static SortKeyRange between(RangeBoundary start, RangeBoundary end) {
    return new SortKeyRange(Optional.of(start), Optional.of(end));
  }

  public static SortKeyRange from(RangeBoundary start) {
    return new SortKeyRange(Optional.of(start), Optional.empty());
  }

  public static SortKeyRange to(RangeBoundary end) {
    return new SortKeyRange(Optional.empty(), Optional.of(end));
  }
}
