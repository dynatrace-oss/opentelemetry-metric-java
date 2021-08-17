package com.dynatrace.opentelemetry.metric;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.opentelemetry.api.metrics.common.Labels;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
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

  private DoublePointData createDoublePointData(Double value, int offset, Labels labels) {
    return DoublePointData.create(
        1619687659000000000L,
        1619687659000000000L + Duration.ofSeconds(offset).toNanos(),
        labels,
        value);
  }

  private DoublePointData createDoublePointData(Double value, int offset) {
    return createDoublePointData(value, offset, Labels.empty());
  }

  private DoublePointData createDoublePointData(Double value) {
    return createDoublePointData(value, 0, Labels.empty());
  }

  private LongPointData createLongPointData(Long value, int offset, Labels labels) {
    return LongPointData.create(
        1619687659000000000L,
        1619687659000000000L + Duration.ofSeconds(offset).toNanos(),
        labels,
        value);
  }

  private LongPointData createLongPointData(Long value, int offset) {
    return createLongPointData(value, offset, Labels.empty());
  }

  private LongPointData createLongPointData(Long value) {
    return createLongPointData(value, 0, Labels.empty());
  }

  @Test
  void testConvertTotalCounterToDeltaAndUpdateCacheDouble() {
    // Note that the time is only used to make sure that the data points are in order, but not
    // to expel data from the Cache.

    // Weird increments, as others lead to doubles with many decimal places due to double
    // imprecision in the subtractions.
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(100.2, 0)))
        .isEqualTo(100.2);
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(200.5, 1)))
        .isEqualTo(100.3);
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(300.87, 2)))
        .isEqualTo(100.37);

    try {
      Thread.sleep(100);
    } catch (InterruptedException ignored) {
    }

    // after the timeout the map is reset.
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(400.4, 3)))
        .isEqualTo(400.4);
  }

  @Test
  void testDoubleInvalidNumbers() {
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(Double.NaN)))
        .isNaN();
    assertThat(
            converter.convertDoubleTotalToDelta(
                "test", createDoublePointData(Double.POSITIVE_INFINITY)))
        .isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(
            converter.convertDoubleTotalToDelta(
                "test", createDoublePointData(Double.NEGATIVE_INFINITY)))
        .isEqualTo(Double.NEGATIVE_INFINITY);
  }

  @Test
  void testDoubleInvalidNumbersResetMap() {
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(100.2, 0)))
        .isEqualTo(100.2);
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(Double.NaN, 1)))
        .isNaN();
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(200.5, 2)))
        .isEqualTo(200.5);
    assertThat(
            converter.convertDoubleTotalToDelta(
                "test", createDoublePointData(Double.POSITIVE_INFINITY, 3)))
        .isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(300.5, 4)))
        .isEqualTo(300.5);
    assertThat(
            converter.convertDoubleTotalToDelta(
                "test", createDoublePointData(Double.NEGATIVE_INFINITY, 5)))
        .isEqualTo(Double.NEGATIVE_INFINITY);
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(400.5, 6)))
        .isEqualTo(400.5);
  }

  @Test
  void testConvertTotalCounterToDeltaAndUpdateCacheLong() {
    assertThat(converter.convertLongTotalToDelta("test", createLongPointData(100L, 0)))
        .isEqualTo(100L);
    assertThat(converter.convertLongTotalToDelta("test", createLongPointData(200L, 1)))
        .isEqualTo(100L);
    assertThat(converter.convertLongTotalToDelta("test", createLongPointData(300L, 2)))
        .isEqualTo(100L);

    try {
      Thread.sleep(100);
    } catch (InterruptedException ignored) {
    }

    assertThat(converter.convertLongTotalToDelta("test", createLongPointData(400L, 3)))
        .isEqualTo(400L);
  }

  @Test
  public void testLabelsAreSortedLong() {
    // a delta is correctly calculated for the seconds metric, meaning that these two metrics are
    // considered equal by the delta calculation
    assertThat(
            converter.convertLongTotalToDelta(
                "test", createLongPointData(100L, 0, Labels.of("l1", "v1", "l2", "v2"))))
        .isEqualTo(100L);
    assertThat(
            converter.convertLongTotalToDelta(
                "test", createLongPointData(200L, 1, Labels.of("l2", "v2", "l1", "v1"))))
        .isEqualTo(100L);
  }

  @Test
  public void testLabelsAreSortedDouble() {
    // a delta is correctly calculated for the seconds metric, meaning that these two metrics are
    // considered equal by the delta calculation
    assertThat(
            converter.convertDoubleTotalToDelta(
                "test", createDoublePointData(100.2, 0, Labels.of("l1", "v1", "l2", "v2"))))
        .isEqualTo(100.2);
    assertThat(
            converter.convertDoubleTotalToDelta(
                "test", createDoublePointData(200.5, 1, Labels.of("l2", "v2", "l1", "v1"))))
        .isEqualTo(100.3);
  }

  @Test
  public void testDoubleAndLongDoNotInterfere() {
    // both counters are called test but do not interfere due to the different data types.
    assertThat(converter.convertLongTotalToDelta("test", createLongPointData(100L)))
        .isEqualTo(100L);
    assertThat(converter.convertLongTotalToDelta("test", createLongPointData(200L)))
        .isEqualTo(100L);

    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(100.2)))
        .isEqualTo(100.2);
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(200.5)))
        .isEqualTo(100.3);
  }
}
