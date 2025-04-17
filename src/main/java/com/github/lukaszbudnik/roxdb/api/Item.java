package com.github.lukaszbudnik.roxdb.api;

import java.util.Map;

public record Item(Key key, Map<String, Object> attributes) {}
