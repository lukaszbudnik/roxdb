package com.github.lukaszbudnik.roxdb.metrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricsConfigReader {
  private static final Logger logger = LoggerFactory.getLogger(MetricsConfigReader.class);
  private static final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

  public MetricsConfig readConfig(String yamlFile) {
    try {
      String configContent = Files.readString(Path.of(yamlFile));
      return mapper.readValue(configContent, MetricsConfig.class);
    } catch (IOException e) {
      logger.warn("Failed to read metrics config", e);
      throw new IllegalArgumentException(e);
    }
  }
}
