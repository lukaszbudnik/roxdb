package com.github.lukaszbudnik.roxdb.rocksdb;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;

public class SerDeUtils {
  private static final Kryo kryo;
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(SerDeUtils.class);

  static {
    kryo = new Kryo();
    kryo.register(HashMap.class);
  }

  public static byte[] serializeKey(Key key) {
    String storageKey = key.partitionKey() + RoxDBImpl.PARTITION_SORT_KEY_SEPARATOR + key.sortKey();
    return storageKey.getBytes(StandardCharsets.UTF_8);
  }

  public static byte[] serializeAttributes(Item item) {
    byte[] value = null;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos)) {
      kryo.writeObject(output, item.attributes());
      // needs explicit flush
      output.flush();
      value = baos.toByteArray();
    } catch (IOException e) {
      String storageKey =
          item.key().partitionKey() + RoxDBImpl.PARTITION_SORT_KEY_SEPARATOR + item.key().sortKey();
      logger.error("Error serializing item: {}", storageKey, e);
      throw new RuntimeException(e);
    }
    return value;
  }

  public static Map<String, Object> deserializeAttributes(byte[] value) {
    Map<String, Object> attributes = null;
    try (Input input = new Input(value)) {
      attributes = kryo.readObject(input, HashMap.class);
    }
    return attributes;
  }
}
