package com.github.lukaszbudnik.roxdb.grpc;

public enum Error {
  ROCKS_DB_ERROR(1, "RocksDB error");

  private final int code;
  private final String message;

  Error(int code, String message) {
    this.code = code;
    this.message = message;
  }

  public int getCode() {
    return code;
  }

  public String getMessage() {
    return message;
  }
}
