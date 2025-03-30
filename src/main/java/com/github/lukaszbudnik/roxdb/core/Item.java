package com.github.lukaszbudnik.roxdb.core;

import java.util.Map;

public record Item(Key key, Map<String, Object> attributes) {
}
