package com.github.lukaszbudnik.roxdb.rocksdb;

import java.util.Map;

public record Item(Key key, Map<String, Object> attributes) {}
