package com.github.lukaszbudnik.roxdb.metrics;

import static org.junit.jupiter.api.Assertions.*;
import static org.rocksdb.HistogramType.*;
import static org.rocksdb.TickerType.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.rocksdb.HistogramType;
import org.rocksdb.TickerType;

public class MetricsConfigProcessorTest {

  @Test
  void getTickerTypes() {
    TickerType[] tickerTypes =
        new TickerType[] {
          BLOCK_CACHE_HIT,
          BLOCK_CACHE_MISS,
          BYTES_WRITTEN,
          BYTES_READ,
          MEMTABLE_HIT,
          MEMTABLE_MISS,
          STALL_MICROS,
          WRITE_WITH_WAL,
          READ_AMP_ESTIMATE_USEFUL_BYTES,
          READ_AMP_TOTAL_READ_BYTES,
          NO_FILE_OPENS,
          NO_FILE_ERRORS,
          COMPACT_READ_BYTES,
          COMPACT_WRITE_BYTES
        };
    List<String> tickerTypesAsStrings = Arrays.stream(tickerTypes).map(TickerType::name).toList();
    MetricsConfig config = new MetricsConfig("", 1, tickerTypesAsStrings, List.of());

    MetricsConfigProcessor processor = new MetricsConfigProcessor(config);
    assertEquals(tickerTypes.length, processor.getTickerTypes().size());
  }

  @Test
  void getHistogramTypes() {
    HistogramType[] histogramTypes =
        new HistogramType[] {
          DB_GET, DB_WRITE, COMPACTION_TIME, TABLE_SYNC_MICROS, COMPRESSION_TIMES_NANOS
        };
    List<String> histogramTypesAsStrings =
        Arrays.stream(histogramTypes).map(HistogramType::name).toList();

    MetricsConfig config = new MetricsConfig("", 1, List.of(), histogramTypesAsStrings);

    MetricsConfigProcessor processor = new MetricsConfigProcessor(config);
    assertEquals(histogramTypes.length, processor.getHistogramTypes().size());
  }

  @Test
  void getTickerTypesAndHistogramTypes() {
    TickerType[] tickerTypes =
        new TickerType[] {
          BLOCK_CACHE_HIT,
          BLOCK_CACHE_MISS,
          BYTES_WRITTEN,
          BYTES_READ,
          MEMTABLE_HIT,
          MEMTABLE_MISS,
          STALL_MICROS,
          WRITE_WITH_WAL,
          READ_AMP_ESTIMATE_USEFUL_BYTES,
          READ_AMP_TOTAL_READ_BYTES,
          NO_FILE_OPENS,
          NO_FILE_ERRORS,
          COMPACT_READ_BYTES,
          COMPACT_WRITE_BYTES
        };
    List<String> tickerTypesAsStrings = Arrays.stream(tickerTypes).map(TickerType::name).toList();

    HistogramType[] histogramTypes =
        new HistogramType[] {
          DB_GET, DB_WRITE, COMPACTION_TIME, TABLE_SYNC_MICROS, COMPRESSION_TIMES_NANOS
        };
    List<String> histogramTypesAsStrings =
        Arrays.stream(histogramTypes).map(HistogramType::name).toList();

    MetricsConfig config = new MetricsConfig("", 1, tickerTypesAsStrings, histogramTypesAsStrings);

    MetricsConfigProcessor processor = new MetricsConfigProcessor(config);
    assertEquals(tickerTypes.length, processor.getTickerTypes().size());
    assertEquals(histogramTypes.length, processor.getHistogramTypes().size());
  }

  @Test
  void getTickerTypesAndHistogramTypesAndInvalidTypes() {

    List<String> tickerTypesAsStrings =
        new ArrayList<>() {
          {
            add("BLOCK_CACHE_HIT");
            add("INCORRECT_TICKER_NAME");
          }
        };

    List<String> histogramTypesAsStrings =
        new ArrayList<>() {
          {
            add("DB_GET");
            add("INCORRECT_HISTOGRAM_NAME");
          }
        };

    MetricsConfig config = new MetricsConfig("", 1, tickerTypesAsStrings, histogramTypesAsStrings);

    MetricsConfigProcessor processor = new MetricsConfigProcessor(config);
    assertEquals(1, processor.getTickerTypes().size());
    assertEquals(1, processor.getHistogramTypes().size());
    assertEquals(TickerType.BLOCK_CACHE_HIT, processor.getTickerTypes().get(0));
    assertEquals(HistogramType.DB_GET, processor.getHistogramTypes().get(0));
  }

  @Test
  void getAllTickerTypesAndAllHistogramTypes() {
    MetricsConfig config = new MetricsConfig("", 1, List.of("*"), List.of("*"));

    MetricsConfigProcessor processor = new MetricsConfigProcessor(config);
    assertEquals(TickerType.values().length, processor.getTickerTypes().size());
    assertEquals(HistogramType.values().length, processor.getHistogramTypes().size());
  }
}
