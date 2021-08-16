package com.dynatrace.opentelemetry.metric;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CumulativeToDeltaConverterTest {
  private final CumulativeToDeltaConverter converter =
      new CumulativeToDeltaConverter(Duration.ofMillis(100));

  @BeforeEach
  void setUp() {
    converter.reset();
  }

  @Test
  void testConvertTotalCounterToDeltaAndUpdateCacheDouble() {
    // Weird increments, as others lead to doubles with many decimal places due to double
    // imprecision in the subtractions.
    assertThat(converter.convertTotalCounterToDeltaAndUpdateCacheDouble("test", 100.2))
        .isEqualTo(100.2);
    assertThat(converter.convertTotalCounterToDeltaAndUpdateCacheDouble("test", 200.5))
        .isEqualTo(100.3);
    assertThat(converter.convertTotalCounterToDeltaAndUpdateCacheDouble("test", 300.87))
        .isEqualTo(100.37);

    try {
      Thread.sleep(100);
    } catch (InterruptedException ignored) {
    }

    assertThat(converter.convertTotalCounterToDeltaAndUpdateCacheDouble("test", 400.4))
        .isEqualTo(400.4);
  }

  @Test
  void testDoubleInvalidNumbers() {
    assertThat(converter.convertTotalCounterToDeltaAndUpdateCacheDouble("test", Double.NaN))
        .isNaN();
    assertThat(
            converter.convertTotalCounterToDeltaAndUpdateCacheDouble(
                "test", Double.POSITIVE_INFINITY))
        .isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(
            converter.convertTotalCounterToDeltaAndUpdateCacheDouble(
                "test", Double.NEGATIVE_INFINITY))
        .isEqualTo(Double.NEGATIVE_INFINITY);
  }

  @Test
  void testDoubleInvalidNumbersIgnored() {
    assertThat(converter.convertTotalCounterToDeltaAndUpdateCacheDouble("test", 100.2))
        .isEqualTo(100.2);
    assertThat(converter.convertTotalCounterToDeltaAndUpdateCacheDouble("test", Double.NaN))
        .isNaN();
    assertThat(
            converter.convertTotalCounterToDeltaAndUpdateCacheDouble(
                "test", Double.POSITIVE_INFINITY))
        .isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(
            converter.convertTotalCounterToDeltaAndUpdateCacheDouble(
                "test", Double.NEGATIVE_INFINITY))
        .isEqualTo(Double.NEGATIVE_INFINITY);
    assertThat(converter.convertTotalCounterToDeltaAndUpdateCacheDouble("test", 200.5))
        .isEqualTo(100.3);
  }

  @Test
  void testConvertTotalCounterToDeltaAndUpdateCacheLong() {
    assertThat(converter.convertTotalCounterToDeltaAndUpdateCacheLong("test", 100L))
        .isEqualTo(100L);
    assertThat(converter.convertTotalCounterToDeltaAndUpdateCacheLong("test", 200L))
        .isEqualTo(100L);
    assertThat(converter.convertTotalCounterToDeltaAndUpdateCacheLong("test", 300L))
        .isEqualTo(100L);

    try {
      Thread.sleep(100);
    } catch (InterruptedException ignored) {
    }

    assertThat(converter.convertTotalCounterToDeltaAndUpdateCacheLong("test", 400L))
        .isEqualTo(400L);
  }
}
