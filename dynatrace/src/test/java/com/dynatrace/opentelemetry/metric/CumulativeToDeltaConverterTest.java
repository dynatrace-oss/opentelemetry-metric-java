/**
 * Copyright 2021 Dynatrace LLC
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dynatrace.opentelemetry.metric;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import java.time.Duration;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CumulativeToDeltaConverterTest {
  private final CumulativeToDeltaConverter converter =
      new CumulativeToDeltaConverter(Duration.ofMillis(100));
  private final Offset<Double> offset = Offset.offset(0.05);

  @BeforeEach
  void setUp() {
    converter.reset();
  }

  private DoublePointData createDoublePointData(Double value, int offset, Attributes attributes) {
    return DoublePointData.create(
        1619687659000000000L,
        1619687659000000000L + Duration.ofSeconds(offset).toNanos(),
        attributes,
        value);
  }

  private DoublePointData createDoublePointData(Double value, int offset) {
    return createDoublePointData(value, offset, Attributes.empty());
  }

  private DoublePointData createDoublePointData(Double value) {
    return createDoublePointData(value, 0, Attributes.empty());
  }

  private LongPointData createLongPointData(Long value, int offset, Attributes attributes) {
    return LongPointData.create(
        1619687659000000000L,
        1619687659000000000L + Duration.ofSeconds(offset).toNanos(),
        attributes,
        value);
  }

  private LongPointData createLongPointData(Long value, int offset) {
    return createLongPointData(value, offset, Attributes.empty());
  }

  private LongPointData createLongPointData(Long value) {
    return createLongPointData(value, 0, Attributes.empty());
  }

  @Test
  void testConvertTotalCounterToDeltaAndUpdateCacheDouble() {
    // Note that the time is only used to make sure that the data points are in order, but not
    // to expel data from the Cache.
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(100.2, 0)))
        .isNull();
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(200.4, 1)))
        .isCloseTo(100.2, offset);
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(300.7, 2)))
        .isCloseTo(100.3, offset);

    try {
      Thread.sleep(100);
    } catch (InterruptedException ignored) {
    }

    // after the timeout the map is reset.
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(100.3, 3)))
        .isNull();
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(300.4, 4)))
        .isCloseTo(200.1, offset);
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
    // a different counter which should not be affected by the resets
    assertThat(
            converter.convertDoubleTotalToDelta("test_not_reset", createDoublePointData(200.5, 4)))
        .isNull();

    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(200.5, 4)))
        .isNull();
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(300.5, 5)))
        .isCloseTo(100., offset);

    // reset
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(Double.NaN, 1)))
        .isNaN();
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(200.5, 4)))
        .isNull();
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(300.5, 5)))
        .isCloseTo(100., offset);

    // reset
    assertThat(
            converter.convertDoubleTotalToDelta(
                "test", createDoublePointData(Double.POSITIVE_INFINITY, 3)))
        .isEqualTo(Double.POSITIVE_INFINITY);
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(200.5, 4)))
        .isNull();
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(300.5, 5)))
        .isCloseTo(100., offset);

    // reset
    assertThat(
            converter.convertDoubleTotalToDelta(
                "test", createDoublePointData(Double.NEGATIVE_INFINITY, 5)))
        .isEqualTo(Double.NEGATIVE_INFINITY);
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(200.5, 4)))
        .isNull();
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(300.5, 5)))
        .isCloseTo(100., offset);

    // check unaffected counter
    assertThat(
            converter.convertDoubleTotalToDelta("test_not_reset", createDoublePointData(300.7, 5)))
        .isCloseTo(100.2, offset);
  }

  @Test
  void testConvertTotalCounterToDeltaAndUpdateCacheLong() {
    assertThat(converter.convertLongTotalToDelta("test", createLongPointData(100L, 0))).isNull();
    assertThat(converter.convertLongTotalToDelta("test", createLongPointData(200L, 1)))
        .isEqualTo(100L);
    assertThat(converter.convertLongTotalToDelta("test", createLongPointData(300L, 2)))
        .isEqualTo(100L);

    try {
      Thread.sleep(100);
    } catch (InterruptedException ignored) {
    }

    assertThat(converter.convertLongTotalToDelta("test", createLongPointData(400L, 3))).isNull();
  }

  @Test
  void testAttributesAreSortedLong() {
    // a delta is correctly calculated for the seconds metric, meaning that these two metrics are
    // considered equal by the delta calculation
    assertThat(
            converter.convertLongTotalToDelta(
                "test",
                createLongPointData(
                    100L, 0, Attributes.of(stringKey("attr1"), "v1", stringKey("attr2"), "v2"))))
        .isNull();
    assertThat(
            converter.convertLongTotalToDelta(
                "test",
                createLongPointData(
                    200L, 1, Attributes.of(stringKey("attr2"), "v2", stringKey("attr1"), "v1"))))
        .isEqualTo(100L);
  }

  @Test
  void testAttributesAreSortedDouble() {
    // a delta is correctly calculated for the seconds metric, meaning that these two metrics are
    // considered equal by the delta calculation
    assertThat(
            converter.convertDoubleTotalToDelta(
                "test",
                createDoublePointData(
                    100.2, 0, Attributes.of(stringKey("attr1"), "v1", stringKey("attr2"), "v2"))))
        .isNull();
    assertThat(
            converter.convertDoubleTotalToDelta(
                "test",
                createDoublePointData(
                    200.5, 1, Attributes.of(stringKey("attr2"), "v2", stringKey("attr1"), "v1"))))
        .isEqualTo(100.3);
  }

  @Test
  void testDoubleAndLongDoNotInterfere() {
    // both counters are called test but do not interfere due to the different data types.
    assertThat(converter.convertLongTotalToDelta("test", createLongPointData(100L))).isNull();
    assertThat(converter.convertLongTotalToDelta("test", createLongPointData(200L)))
        .isEqualTo(100L);

    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(100.2))).isNull();
    assertThat(converter.convertDoubleTotalToDelta("test", createDoublePointData(200.5)))
        .isCloseTo(100.3, offset);
  }

  @Test
  void testCumulativeToDeltaWithDuplicatedAttributes() {
    assertThat(
            converter.convertLongTotalToDelta(
                "test",
                createLongPointData(
                    100L, 0, Attributes.of(stringKey("attr1"), "v1", stringKey("attr2"), "v2"))))
        .isNull();
    assertThat(
            converter.convertLongTotalToDelta(
                "test",
                createLongPointData(
                    200L,
                    1,
                    Attributes.of(
                        stringKey("attr1"),
                        "some value",
                        stringKey("attr2"),
                        "v2",
                        stringKey("attr1"),
                        "v1"))))
        .isEqualTo(100L);
  }

  @Test
  void testAttributesAreDeDuplicateAndSorted() {
    // Our converter relies on the sorting and deduplication to properly
    // create identifiers out of metrics. For this reason, we have this test.
    Attributes attributes =
        Attributes.of(
            stringKey("attr2"), "v2", stringKey("attr1"), "some value", stringKey("attr1"), "v1");

    Object[] keys = attributes.asMap().keySet().toArray();

    assertEquals("attr1", keys[0].toString());
    assertEquals("v1", attributes.get(stringKey("attr1")));

    assertEquals("attr2", keys[1].toString());
    assertEquals("v2", attributes.get(stringKey("attr2")));
  }
}
