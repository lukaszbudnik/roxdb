package com.github.lukaszbudnik.roxdb.grpc;

import static org.junit.jupiter.api.Assertions.*;

import com.github.lukaszbudnik.roxdb.rocksdb.RangeType;
import com.github.lukaszbudnik.roxdb.rocksdb.SortKeyRange;
import com.github.lukaszbudnik.roxdb.v1.Item;
import com.github.lukaszbudnik.roxdb.v1.ItemRequest;
import com.github.lukaszbudnik.roxdb.v1.Key;
import com.google.protobuf.Struct;
import java.util.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ProtoUtilsTest {

  @Test
  @DisplayName("Should convert primitive types between Struct and Map")
  void testPrimitiveTypesConversion() {
    // Prepare test data
    Map<String, Object> originalMap = new HashMap<>();
    originalMap.put("string", "test");
    originalMap.put("number", 42.0);
    originalMap.put("boolean", true);
    originalMap.put("null", null);

    // Convert Map to Struct
    Struct struct = ProtoUtils.mapToStruct(originalMap);

    // Convert back to Map
    Map<String, Object> resultMap = ProtoUtils.structToMap(struct);

    // Verify
    assertEquals(originalMap, resultMap);
  }

  @Test
  @DisplayName("Should convert nested structures between Struct and Map")
  void testNestedStructuresConversion() {
    // Prepare nested test data
    Map<String, Object> nestedMap = new HashMap<>();
    nestedMap.put("nested_string", "nested value");

    List<Object> list = Arrays.asList("item1", 42.0, true);

    Map<String, Object> originalMap = new HashMap<>();
    originalMap.put("map", nestedMap);
    originalMap.put("list", list);

    // Convert to Struct and back
    Struct struct = ProtoUtils.mapToStruct(originalMap);
    Map<String, Object> resultMap = ProtoUtils.structToMap(struct);

    // Verify
    assertEquals(originalMap, resultMap);
  }

  @Test
  @DisplayName("Should throw IllegalArgumentException for unsupported types")
  void testUnsupportedType() {
    Map<String, Object> map = new HashMap<>();
    map.put("unsupported", new StringBuilder()); // StringBuilder is not supported

    assertThrows(IllegalArgumentException.class, () -> ProtoUtils.mapToStruct(map));
  }

  @Test
  @DisplayName("Should convert between Item model and proto")
  void testItemConversion() {
    // Prepare test data
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("test_key", "test_value");

    com.github.lukaszbudnik.roxdb.rocksdb.Key modelKey =
        new com.github.lukaszbudnik.roxdb.rocksdb.Key("partition1", "sort1");
    com.github.lukaszbudnik.roxdb.rocksdb.Item modelItem =
        new com.github.lukaszbudnik.roxdb.rocksdb.Item(modelKey, attributes);

    // Convert model to proto
    Item protoItem = ProtoUtils.modelToProto(modelItem);

    // Convert proto back to model
    com.github.lukaszbudnik.roxdb.rocksdb.Item resultItem = ProtoUtils.protoToModel(protoItem);

    // Verify
    assertEquals(modelItem.key().partitionKey(), resultItem.key().partitionKey());
    assertEquals(modelItem.key().sortKey(), resultItem.key().sortKey());
    assertEquals(modelItem.attributes(), resultItem.attributes());
  }

  @Test
  @DisplayName("Should convert Key proto to model")
  void testProtoToModel() {
    // Create proto key
    Key protoKey = Key.newBuilder().setPartitionKey("partition1").setSortKey("sort1").build();

    // Convert to model
    com.github.lukaszbudnik.roxdb.rocksdb.Key modelKey = ProtoUtils.protoToModel(protoKey);

    // Verify
    assertEquals(protoKey.getPartitionKey(), modelKey.partitionKey());
    assertEquals(protoKey.getSortKey(), modelKey.sortKey());
  }

  @Test
  @DisplayName("Should handle empty collections")
  void testEmptyCollections() {
    // Test empty map
    Map<String, Object> emptyMap = new HashMap<>();
    Struct emptyStruct = ProtoUtils.mapToStruct(emptyMap);
    Map<String, Object> resultEmptyMap = ProtoUtils.structToMap(emptyStruct);
    assertEquals(emptyMap, resultEmptyMap);

    // Test empty list
    Map<String, Object> mapWithEmptyList = new HashMap<>();
    mapWithEmptyList.put("empty_list", Collections.emptyList());
    Struct structWithEmptyList = ProtoUtils.mapToStruct(mapWithEmptyList);
    Map<String, Object> resultMapWithEmptyList = ProtoUtils.structToMap(structWithEmptyList);
    assertEquals(mapWithEmptyList, resultMapWithEmptyList);
  }

  @Test
  @DisplayName("Should handle complex nested structures")
  void testComplexNestedStructures() {
    // Create a complex nested structure
    Map<String, Object> deeplyNested = new HashMap<>();
    deeplyNested.put("string", "value");
    deeplyNested.put("number", 123.45);

    List<Object> mixedList = Arrays.asList("string", 42.0, true, null, deeplyNested);

    Map<String, Object> originalMap = new HashMap<>();
    originalMap.put("nested_map", deeplyNested);
    originalMap.put("mixed_list", mixedList);

    // Convert to Struct and back
    Struct struct = ProtoUtils.mapToStruct(originalMap);
    Map<String, Object> resultMap = ProtoUtils.structToMap(struct);

    // Verify
    assertEquals(originalMap, resultMap);
  }

  @Test
  public void testProtoToModelSortKeyRangeBetween() {
    // Create a range with both start and end boundaries
    ItemRequest.RangeBoundary startBoundary =
        ItemRequest.RangeBoundary.newBuilder()
            .setValue("start123")
            .setType(ItemRequest.RangeType.INCLUSIVE)
            .build();

    ItemRequest.RangeBoundary endBoundary =
        ItemRequest.RangeBoundary.newBuilder()
            .setValue("end456")
            .setType(ItemRequest.RangeType.EXCLUSIVE)
            .build();

    ItemRequest.SortKeyRange protoRange =
        ItemRequest.SortKeyRange.newBuilder().setStart(startBoundary).setEnd(endBoundary).build();

    SortKeyRange modelRange = ProtoUtils.protoToModel(protoRange);

    assertNotNull(modelRange);
    assertEquals("start123", modelRange.start().get().value());
    assertEquals(RangeType.INCLUSIVE, modelRange.start().get().type());
    assertEquals("end456", modelRange.end().get().value());
    assertEquals(RangeType.EXCLUSIVE, modelRange.end().get().type());
  }

  @Test
  public void testProtoToModelSortKeyRangeFrom() {
    // Create a range with only start boundary
    ItemRequest.RangeBoundary startBoundary =
        ItemRequest.RangeBoundary.newBuilder()
            .setValue("start123")
            .setType(ItemRequest.RangeType.INCLUSIVE)
            .build();

    ItemRequest.SortKeyRange protoRange =
        ItemRequest.SortKeyRange.newBuilder().setStart(startBoundary).build();

    SortKeyRange modelRange = ProtoUtils.protoToModel(protoRange);

    assertNotNull(modelRange);
    assertEquals("start123", modelRange.start().get().value());
    assertEquals(RangeType.INCLUSIVE, modelRange.start().get().type());
    assertTrue(modelRange.end().isEmpty());
  }

  @Test
  public void testProtoToModelSortKeyRangeTo() {
    // Create a range with only end boundary
    ItemRequest.RangeBoundary endBoundary =
        ItemRequest.RangeBoundary.newBuilder()
            .setValue("end456")
            .setType(ItemRequest.RangeType.EXCLUSIVE)
            .build();

    ItemRequest.SortKeyRange protoRange =
        ItemRequest.SortKeyRange.newBuilder().setEnd(endBoundary).build();

    SortKeyRange modelRange = ProtoUtils.protoToModel(protoRange);

    assertNotNull(modelRange);
    assertTrue(modelRange.start().isEmpty());
    assertEquals("end456", modelRange.end().get().value());
    assertEquals(RangeType.EXCLUSIVE, modelRange.end().get().type());
  }

  @Test
  public void testProtoToModelSortKeyRangeWithNoBoundaries() {
    ItemRequest.SortKeyRange protoRange = ItemRequest.SortKeyRange.newBuilder().build();
    assertThrows(IllegalArgumentException.class, () -> ProtoUtils.protoToModel(protoRange));
  }

  @Test
  public void testProtoToModelRangeType() {
    assertEquals(RangeType.EXCLUSIVE, ProtoUtils.protoToModel(ItemRequest.RangeType.EXCLUSIVE));
    assertEquals(RangeType.INCLUSIVE, ProtoUtils.protoToModel(ItemRequest.RangeType.INCLUSIVE));
  }

  @Test
  public void testProtoToModelRangeTypeWithUnsupportedType() {
    // Assuming there are other enum values in ItemRequest.RangeType
    // that are not supported by the conversion
    assertThrows(
        IllegalArgumentException.class,
        () -> ProtoUtils.protoToModel(ItemRequest.RangeType.UNRECOGNIZED));
  }
}
