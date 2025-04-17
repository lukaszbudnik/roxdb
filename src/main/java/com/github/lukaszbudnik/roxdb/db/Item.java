package com.github.lukaszbudnik.roxdb.db;

import java.util.Map;

public record Item(Key key, Map<String, Object> attributes) {}
