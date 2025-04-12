package com.github.lukaszbudnik.roxdb.grpc;

import com.github.lukaszbudnik.roxdb.proto.Item;
import com.github.lukaszbudnik.roxdb.proto.Key;
import com.google.protobuf.ListValue;
import com.google.protobuf.NullValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProtoUtils {
    public static Map<String, Object> structToMap(Struct struct) {
        Map<String, Object> result = new HashMap<>();
        struct.getFieldsMap().forEach((key, value) -> {
            result.put(key, valueToObject(value));
        });
        return result;
    }

    public static Object valueToObject(Value value) {
        return switch (value.getKindCase()) {
            case NUMBER_VALUE -> value.getNumberValue();
            case STRING_VALUE -> value.getStringValue();
            case BOOL_VALUE -> value.getBoolValue();
            case STRUCT_VALUE -> structToMap(value.getStructValue());
            case LIST_VALUE -> value.getListValue().getValuesList().stream()
                                    .map(ProtoUtils::valueToObject)
                                    .collect(Collectors.toList())
            ;
            case NULL_VALUE -> null;
            default -> throw new IllegalArgumentException("Unsupported value type: " + value.getKindCase());
        };
    }

    public static Struct mapToStruct(Map<String, Object> map) {
        Struct.Builder structBuilder = Struct.newBuilder();
        map.forEach((key, value) -> structBuilder.putFields(key, objectToValue(value)));
        return structBuilder.build();
    }

    public static Value objectToValue(Object obj) {
        Value.Builder valueBuilder = Value.newBuilder();

        if (obj == null) {
            return valueBuilder.setNullValue(NullValue.NULL_VALUE).build();
        } else if (obj instanceof String) {
            return valueBuilder.setStringValue((String) obj).build();
        } else if (obj instanceof Number) {
            return valueBuilder.setNumberValue(((Number) obj).doubleValue()).build();
        } else if (obj instanceof Boolean) {
            return valueBuilder.setBoolValue((Boolean) obj).build();
        } else if (obj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) obj;
            return valueBuilder.setStructValue(mapToStruct(map)).build();
        } else if (obj instanceof List) {
            ListValue.Builder listBuilder = ListValue.newBuilder();
            ((List<?>) obj).forEach(item -> listBuilder.addValues(objectToValue(item)));
            return valueBuilder.setListValue(listBuilder.build()).build();
        }

        throw new IllegalArgumentException("Unsupported type: " + obj.getClass());
    }

    public static Item itemToProto(com.github.lukaszbudnik.roxdb.core.Item item) {
        return Item.newBuilder()
                   .setKey(Key.newBuilder()
                              .setPartitionKey(item.key().partitionKey())
                              .setSortKey(item.key().sortKey())
                              .build())
                   .setAttributes(ProtoUtils.mapToStruct(item.attributes()))
                   .build();
    }
}
