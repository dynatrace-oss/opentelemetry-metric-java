/*
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

import static com.dynatrace.opentelemetry.metric.TestDataConstants.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dynatrace.metric.util.Dimension;
import com.dynatrace.metric.util.DimensionList;
import com.dynatrace.metric.util.MetricBuilderFactory;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.internal.data.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SerializerTest {
  private Serializer serializer = null;

  @BeforeEach
  void setUp() {
    serializer = new Serializer(MetricBuilderFactory.builder().build());
  }

  @Test
  void fromAttributes() {
    Attributes attributes =
        Attributes.builder().put("attr1", "value1").put("attr2", "value2").build();

    DimensionList expected =
        DimensionList.create(
            Dimension.create("attr1", "value1"), Dimension.create("attr2", "value2"));

    DimensionList actual = Serializer.fromAttributes(attributes);

    assertThat(actual.getDimensions()).containsOnlyElementsOf(expected.getDimensions());
    assertThat(actual).isNotSameAs(expected);
  }

  @Test
  void dropTypedAttributes() {
    Attributes attributes =
        Attributes.builder().put("attr1", 1).put("attr2", 1.5).put("attr3", true).build();

    DimensionList actual = Serializer.fromAttributes(attributes);

    assertThat(actual.getDimensions()).isEmpty();
  }

  @Test
  void dropTypedAttributesArrayValues() {
    Attributes attributes =
        Attributes.builder()
            .put("attr1", "v1", "v2")
            .put("attr2", 1, 2)
            .put("attr3", 1.1, 2.2)
            .put("attr4", true, false)
            .build();

    DimensionList actual = Serializer.fromAttributes(attributes);

    assertThat(actual.getDimensions()).isEmpty();
  }

  @Test
  void fromAttributesEmpty() {
    Attributes attributes = Attributes.empty();

    DimensionList actual = Serializer.fromAttributes(attributes);

    assertTrue(actual.getDimensions().isEmpty());
  }

  @Test
  void fromAttributesNormalize() {
    Attributes attributes =
        Attributes.builder().put("~~!123", "test").put("!!test2", "test2").build();

    DimensionList actual = Serializer.fromAttributes(attributes);

    assertThat(actual.getDimensions())
        .hasSize(2)
        .containsExactlyInAnyOrder(
            Dimension.create("_", "test"), Dimension.create("_test2", "test2"));
  }

  @Test
  void createSumLines_Double_Cumulative_Monotonic() {
    MetricData metricData =
        ImmutableMetricData.createDoubleSum(
            DEFAULT_RESOURCE,
            DEFAULT_SCOPE,
            DEFAULT_NAME,
            DEFAULT_DESC,
            DEFAULT_UNIT,
            ImmutableSumData.create(
                true,
                AggregationTemporality.CUMULATIVE,
                Arrays.asList(
                    ImmutableDoublePointData.create(
                        NANOS_TS_1, NANOS_TS_2, EMPTY_ATTRIBUTES, 123.7),
                    ImmutableDoublePointData.create(
                        NANOS_TS_1, NANOS_TS_3, EMPTY_ATTRIBUTES, 321.9),
                    ImmutableDoublePointData.create(0L, 0L, EMPTY_ATTRIBUTES, 456.7))));

    // Cumulative monotonic counters are transformed to Delta monotonic counters, losing the first
    // data point as a reference
    List<String> lines = serializer.createDoubleSumLines(metricData);
    assertThat(lines)
        .hasSize(2)
        .containsExactly(
            String.format("%s count,delta=198.2 %d", DEFAULT_NAME, MILLIS_TS_3),
            String.format("%s count,delta=134.8", DEFAULT_NAME));
  }

  @Test
  void createSumLines_Double_Cumulative_NonMonotonic() {
    MetricData metricData =
        ImmutableMetricData.createDoubleSum(
            DEFAULT_RESOURCE,
            DEFAULT_SCOPE,
            DEFAULT_NAME,
            DEFAULT_DESC,
            DEFAULT_UNIT,
            ImmutableSumData.create(
                false,
                AggregationTemporality.CUMULATIVE,
                Arrays.asList(
                    ImmutableDoublePointData.create(
                        NANOS_TS_1, NANOS_TS_2, EMPTY_ATTRIBUTES, 123.7),
                    ImmutableDoublePointData.create(NANOS_TS_1, NANOS_TS_3, EMPTY_ATTRIBUTES, 21.9),
                    ImmutableDoublePointData.create(0L, 0L, EMPTY_ATTRIBUTES, 456.7))));

    // Non-monotonic Sums will be converted to Gauge.
    List<String> lines = serializer.createDoubleSumLines(metricData);
    assertThat(lines)
        .hasSize(3)
        .containsExactly(
            String.format("%s gauge,123.7 %d", DEFAULT_NAME, MILLIS_TS_2),
            String.format("%s gauge,21.9 %d", DEFAULT_NAME, MILLIS_TS_3),
            String.format("%s gauge,456.7", DEFAULT_NAME));
  }

  @Test
  void createSumLines_Double_Delta_Monotonic() {
    MetricData metricData =
        ImmutableMetricData.createDoubleSum(
            DEFAULT_RESOURCE,
            DEFAULT_SCOPE,
            DEFAULT_NAME,
            DEFAULT_DESC,
            DEFAULT_UNIT,
            ImmutableSumData.create(
                true,
                AggregationTemporality.DELTA,
                Arrays.asList(
                    ImmutableDoublePointData.create(
                        NANOS_TS_1, NANOS_TS_2, EMPTY_ATTRIBUTES, 123.7),
                    ImmutableDoublePointData.create(
                        NANOS_TS_1, NANOS_TS_3, EMPTY_ATTRIBUTES, 321.9),
                    ImmutableDoublePointData.create(0L, 0L, EMPTY_ATTRIBUTES, 456.7))));

    // Delta monotonic counters are exported as they are.
    List<String> lines = serializer.createDoubleSumLines(metricData);
    assertThat(lines)
        .hasSize(3)
        .containsExactly(
            String.format("%s count,delta=123.7 %d", DEFAULT_NAME, MILLIS_TS_2),
            String.format("%s count,delta=321.9 %d", DEFAULT_NAME, MILLIS_TS_3),
            String.format("%s count,delta=456.7", DEFAULT_NAME));
  }

  @Test
  void createSumLines_Double_Delta_NonMonotonic() {
    MetricData metricData =
        ImmutableMetricData.createDoubleSum(
            DEFAULT_RESOURCE,
            DEFAULT_SCOPE,
            DEFAULT_NAME,
            DEFAULT_DESC,
            DEFAULT_UNIT,
            ImmutableSumData.create(
                false,
                AggregationTemporality.DELTA,
                Arrays.asList(
                    ImmutableDoublePointData.create(
                        NANOS_TS_1, NANOS_TS_2, EMPTY_ATTRIBUTES, 123.7),
                    ImmutableDoublePointData.create(NANOS_TS_1, NANOS_TS_3, EMPTY_ATTRIBUTES, 20.9),
                    ImmutableDoublePointData.create(0L, 0L, EMPTY_ATTRIBUTES, 456.7))));

    List<String> lines = serializer.createDoubleSumLines(metricData);
    // delta non-monotonic sums are currently dropped (requires Delta -> cumulative conversion).
    assertThat(lines).isEmpty();
  }

  @Test
  void createSumLines_Long_Cumulative_Monotonic() {
    MetricData metricData =
        ImmutableMetricData.createLongSum(
            DEFAULT_RESOURCE,
            DEFAULT_SCOPE,
            DEFAULT_NAME,
            DEFAULT_DESC,
            DEFAULT_UNIT,
            ImmutableSumData.create(
                true,
                AggregationTemporality.CUMULATIVE,
                Arrays.asList(
                    ImmutableLongPointData.create(NANOS_TS_1, NANOS_TS_2, EMPTY_ATTRIBUTES, 123),
                    ImmutableLongPointData.create(NANOS_TS_1, NANOS_TS_3, EMPTY_ATTRIBUTES, 321),
                    ImmutableLongPointData.create(0L, 0L, EMPTY_ATTRIBUTES, 456))));

    // Cumulative monotonic counters are transformed to Delta monotonic counters, losing the first
    // data point as a reference
    List<String> lines = serializer.createLongSumLines(metricData);
    assertThat(lines)
        .hasSize(2)
        .containsExactly(
            String.format("%s count,delta=198 %d", DEFAULT_NAME, MILLIS_TS_3),
            String.format("%s count,delta=135", DEFAULT_NAME));
  }

  @Test
  void createSumLines_Long_Cumulative_NonMonotonic() {
    MetricData metricData =
        ImmutableMetricData.createLongSum(
            DEFAULT_RESOURCE,
            DEFAULT_SCOPE,
            DEFAULT_NAME,
            DEFAULT_DESC,
            DEFAULT_UNIT,
            ImmutableSumData.create(
                false,
                AggregationTemporality.CUMULATIVE,
                Arrays.asList(
                    ImmutableLongPointData.create(NANOS_TS_1, NANOS_TS_2, EMPTY_ATTRIBUTES, 123),
                    ImmutableLongPointData.create(NANOS_TS_1, NANOS_TS_3, EMPTY_ATTRIBUTES, 21),
                    ImmutableLongPointData.create(0L, 0L, EMPTY_ATTRIBUTES, 456))));

    // Non-monotonic Sums will be converted to Gauge.
    List<String> lines = serializer.createLongSumLines(metricData);
    assertThat(lines)
        .hasSize(3)
        .containsExactly(
            String.format("%s gauge,123 %d", DEFAULT_NAME, MILLIS_TS_2),
            String.format("%s gauge,21 %d", DEFAULT_NAME, MILLIS_TS_3),
            String.format("%s gauge,456", DEFAULT_NAME));
  }

  @Test
  void createSumLines_Long_Delta_Monotonic() {
    MetricData metricData =
        ImmutableMetricData.createLongSum(
            DEFAULT_RESOURCE,
            DEFAULT_SCOPE,
            DEFAULT_NAME,
            DEFAULT_DESC,
            DEFAULT_UNIT,
            ImmutableSumData.create(
                true,
                AggregationTemporality.DELTA,
                Arrays.asList(
                    ImmutableLongPointData.create(NANOS_TS_1, NANOS_TS_2, EMPTY_ATTRIBUTES, 123),
                    ImmutableLongPointData.create(NANOS_TS_1, NANOS_TS_3, EMPTY_ATTRIBUTES, 321),
                    ImmutableLongPointData.create(0L, 0L, EMPTY_ATTRIBUTES, 456))));

    List<String> longSumLines = serializer.createLongSumLines(metricData);
    assertThat(longSumLines)
        .hasSize(3)
        .containsExactly(
            String.format("%s count,delta=123 %d", DEFAULT_NAME, MILLIS_TS_2),
            String.format("%s count,delta=321 %d", DEFAULT_NAME, MILLIS_TS_3),
            String.format("%s count,delta=456", DEFAULT_NAME));
  }

  @Test
  void createSumLines_Long_Delta_NonMonotonic() {
    MetricData metricData =
        ImmutableMetricData.createLongSum(
            DEFAULT_RESOURCE,
            DEFAULT_SCOPE,
            DEFAULT_NAME,
            DEFAULT_DESC,
            DEFAULT_UNIT,
            ImmutableSumData.create(
                false,
                AggregationTemporality.DELTA,
                Arrays.asList(
                    ImmutableLongPointData.create(NANOS_TS_1, NANOS_TS_2, EMPTY_ATTRIBUTES, 123),
                    ImmutableLongPointData.create(NANOS_TS_1, NANOS_TS_3, EMPTY_ATTRIBUTES, 21),
                    ImmutableLongPointData.create(0L, 0L, EMPTY_ATTRIBUTES, 456))));

    // delta non-monotonic sums are currently dropped (requires Delta -> cumulative conversion).
    List<String> lines = serializer.createLongSumLines(metricData);
    assertThat(lines).isEmpty();
  }

  @Test
  void createGaugeLines_Long() {
    MetricData metricData =
        ImmutableMetricData.createLongGauge(
            DEFAULT_RESOURCE,
            DEFAULT_SCOPE,
            DEFAULT_NAME,
            DEFAULT_DESC,
            DEFAULT_UNIT,
            ImmutableGaugeData.create(
                Arrays.asList(
                    ImmutableLongPointData.create(NANOS_TS_1, NANOS_TS_2, EMPTY_ATTRIBUTES, 123),
                    ImmutableLongPointData.create(NANOS_TS_1, NANOS_TS_3, EMPTY_ATTRIBUTES, 23),
                    ImmutableLongPointData.create(0L, 0L, EMPTY_ATTRIBUTES, 345))));
    List<String> lines = serializer.createLongGaugeLines(metricData);

    assertThat(lines)
        .hasSize(3)
        .containsExactly(
            String.format("%s gauge,123 %s", DEFAULT_NAME, MILLIS_TS_2),
            String.format("%s gauge,23 %s", DEFAULT_NAME, MILLIS_TS_3),
            String.format("%s gauge,345", DEFAULT_NAME));
  }

  @Test
  void createGaugeLines_Double() {
    MetricData metricData =
        ImmutableMetricData.createDoubleGauge(
            DEFAULT_RESOURCE,
            DEFAULT_SCOPE,
            DEFAULT_NAME,
            DEFAULT_DESC,
            DEFAULT_UNIT,
            ImmutableGaugeData.create(
                Arrays.asList(
                    ImmutableDoublePointData.create(
                        NANOS_TS_1, NANOS_TS_2, EMPTY_ATTRIBUTES, 123.4),
                    ImmutableDoublePointData.create(NANOS_TS_1, NANOS_TS_3, EMPTY_ATTRIBUTES, 23.5),
                    ImmutableDoublePointData.create(0L, 0L, EMPTY_ATTRIBUTES, 345.6))));
    List<String> lines = serializer.createDoubleGaugeLines(metricData);

    assertThat(lines)
        .hasSize(3)
        .containsExactly(
            String.format("%s gauge,123.4 %s", DEFAULT_NAME, MILLIS_TS_2),
            String.format("%s gauge,23.5 %s", DEFAULT_NAME, MILLIS_TS_3),
            String.format("%s gauge,345.6", DEFAULT_NAME));
  }

  @Test
  void createDoubleSummaryLines() {
    MetricData metricData =
        ImmutableMetricData.createDoubleSummary(
            DEFAULT_RESOURCE,
            DEFAULT_SCOPE,
            DEFAULT_NAME,
            DEFAULT_DESC,
            DEFAULT_UNIT,
            ImmutableSummaryData.create(
                Arrays.asList(
                    ImmutableSummaryPointData.create(
                        NANOS_TS_1,
                        NANOS_TS_2,
                        EMPTY_ATTRIBUTES,
                        4,
                        100.4,
                        Arrays.asList(
                            ImmutableValueAtQuantile.create(.5, 15.1),
                            ImmutableValueAtQuantile.create(.9, 24.2))),
                    ImmutableSummaryPointData.create(
                        NANOS_TS_1,
                        NANOS_TS_3,
                        EMPTY_ATTRIBUTES,
                        5,
                        125.5,
                        Arrays.asList(
                            ImmutableValueAtQuantile.create(.5, 15.12),
                            ImmutableValueAtQuantile.create(.9, 24.23))))));

    // Quantiles are dropped, only a summary gauge with min=max=sum/count and sum & count is
    // exported
    List<String> lines = serializer.createDoubleSummaryLines(metricData);
    assertThat(lines)
        .hasSize(2)
        .containsExactly(
            String.format(
                "%s gauge,min=25.1,max=25.1,sum=100.4,count=4 %s", DEFAULT_NAME, MILLIS_TS_2),
            String.format(
                "%s gauge,min=25.1,max=25.1,sum=125.5,count=5 %s", DEFAULT_NAME, MILLIS_TS_3));
  }

  @Test
  void createHistogramLines_Delta() {
    MetricData metricData =
        ImmutableMetricData.createDoubleHistogram(
            DEFAULT_RESOURCE,
            DEFAULT_SCOPE,
            DEFAULT_NAME,
            DEFAULT_DESC,
            DEFAULT_UNIT,
            ImmutableHistogramData.create(
                AggregationTemporality.DELTA,
                Arrays.asList(
                    ImmutableHistogramPointData.create(
                        NANOS_TS_1,
                        NANOS_TS_2,
                        EMPTY_ATTRIBUTES,
                        125.5,
                        null,
                        null,
                        Arrays.asList(0.0, 12.5, 50.7),
                        Arrays.asList(0L, 5L, 3L, 0L)),
                    ImmutableHistogramPointData.create(
                        NANOS_TS_1,
                        NANOS_TS_3,
                        EMPTY_ATTRIBUTES,
                        100.4,
                        1.2,
                        32.6,
                        Arrays.asList(2.3, 11.4, 23.6),
                        Arrays.asList(1L, 7L, 4L, 1L)))));

    List<String> lines = serializer.createDoubleHistogramLines(metricData);
    assertThat(lines)
        .hasSize(2)
        .containsExactly(
            // min&max estimated from boundaries
            String.format(
                "%s gauge,min=0.0,max=50.7,sum=125.5,count=8 %d", DEFAULT_NAME, MILLIS_TS_2),
            // uses explicitly set min&max
            String.format(
                "%s gauge,min=1.2,max=32.6,sum=100.4,count=13 %d", DEFAULT_NAME, MILLIS_TS_3));
  }

  @Test
  void createHistogramLines_Cumulative() {
    MetricData metricData =
        ImmutableMetricData.createDoubleHistogram(
            DEFAULT_RESOURCE,
            DEFAULT_SCOPE,
            DEFAULT_NAME,
            DEFAULT_DESC,
            DEFAULT_UNIT,
            ImmutableHistogramData.create(
                AggregationTemporality.CUMULATIVE,
                Arrays.asList(
                    ImmutableHistogramPointData.create(
                        NANOS_TS_1,
                        NANOS_TS_2,
                        EMPTY_ATTRIBUTES,
                        125.5,
                        null,
                        null,
                        Arrays.asList(0.0, 12.5, 50.7),
                        Arrays.asList(0L, 5L, 3L, 0L)),
                    ImmutableHistogramPointData.create(
                        NANOS_TS_1,
                        NANOS_TS_3,
                        EMPTY_ATTRIBUTES,
                        100.4,
                        1.2,
                        32.6,
                        Arrays.asList(2.3, 11.4, 23.6),
                        Arrays.asList(1L, 7L, 4L, 1L)))));

    List<String> lines = serializer.createDoubleHistogramLines(metricData);
    assertThat(lines).isEmpty();
  }

  @Test
  void createInvalidDoubleSumLinesCumulative() {
    MetricData metricData =
        ImmutableMetricData.createDoubleSum(
            DEFAULT_RESOURCE,
            DEFAULT_SCOPE,
            DEFAULT_NAME,
            DEFAULT_DESC,
            DEFAULT_UNIT,
            ImmutableSumData.create(
                false,
                AggregationTemporality.DELTA,
                Arrays.asList(
                    ImmutableDoublePointData.create(
                        NANOS_TS_1, NANOS_TS_2, EMPTY_ATTRIBUTES, Double.NaN),
                    ImmutableDoublePointData.create(
                        NANOS_TS_1, NANOS_TS_2, EMPTY_ATTRIBUTES, Double.NEGATIVE_INFINITY),
                    ImmutableDoublePointData.create(
                        NANOS_TS_1, NANOS_TS_2, EMPTY_ATTRIBUTES, Double.POSITIVE_INFINITY))));

    List<String> doubleSumLines = serializer.createDoubleSumLines(metricData);
    assertThat(doubleSumLines).isEmpty();
  }

  @Test
  void createInvalidDoubleSummaryLines() {
    MetricData metricData =
        ImmutableMetricData.createDoubleSummary(
            DEFAULT_RESOURCE,
            DEFAULT_SCOPE,
            DEFAULT_NAME,
            DEFAULT_DESC,
            DEFAULT_UNIT,
            ImmutableSummaryData.create(
                Arrays.asList(
                    ImmutableSummaryPointData.create(
                        NANOS_TS_1,
                        NANOS_TS_2,
                        EMPTY_ATTRIBUTES,
                        3,
                        Double.NaN,
                        Arrays.asList(
                            ImmutableValueAtQuantile.create(0.0, 0.22),
                            ImmutableValueAtQuantile.create(100.0, 123.45))),
                    ImmutableSummaryPointData.create(
                        NANOS_TS_1,
                        NANOS_TS_2,
                        EMPTY_ATTRIBUTES,
                        3,
                        Double.POSITIVE_INFINITY,
                        Arrays.asList(
                            ImmutableValueAtQuantile.create(0.0, 0.22),
                            ImmutableValueAtQuantile.create(100.0, 123.45))),
                    ImmutableSummaryPointData.create(
                        NANOS_TS_1,
                        NANOS_TS_2,
                        EMPTY_ATTRIBUTES,
                        3,
                        Double.NEGATIVE_INFINITY,
                        Arrays.asList(
                            ImmutableValueAtQuantile.create(0.0, 0.22),
                            ImmutableValueAtQuantile.create(100.0, 123.45))))));

    List<String> doubleSummaryLines = serializer.createDoubleSummaryLines(metricData);
    assertThat(doubleSummaryLines).isEmpty();
  }

  @Test
  void createInvalidDoubleHistogramLines() {
    MetricData metricData =
        ImmutableMetricData.createDoubleHistogram(
            DEFAULT_RESOURCE,
            DEFAULT_SCOPE,
            DEFAULT_NAME,
            DEFAULT_DESC,
            DEFAULT_UNIT,
            ImmutableHistogramData.create(
                AggregationTemporality.DELTA,
                Arrays.asList(
                    // NaN Sum
                    ImmutableHistogramPointData.create(
                        NANOS_TS_1,
                        NANOS_TS_2,
                        EMPTY_ATTRIBUTES,
                        Double.NaN,
                        null,
                        null,
                        Arrays.asList(0.0, 5.0, 10.0),
                        Arrays.asList(0L, 1L, 2L, 3L)),
                    // +Inf sum
                    ImmutableHistogramPointData.create(
                        NANOS_TS_1,
                        NANOS_TS_2,
                        EMPTY_ATTRIBUTES,
                        Double.POSITIVE_INFINITY,
                        null,
                        null,
                        Arrays.asList(0.0, 5.0, 10.0),
                        Arrays.asList(0L, 1L, 2L, 3L)),
                    // -Inf sum
                    ImmutableHistogramPointData.create(
                        NANOS_TS_1,
                        NANOS_TS_2,
                        EMPTY_ATTRIBUTES,
                        Double.NEGATIVE_INFINITY,
                        null,
                        null,
                        Arrays.asList(0.0, 5.0, 10.0),
                        Arrays.asList(0L, 1L, 2L, 3L)))));

    List<String> doubleHistogramLines = serializer.createDoubleHistogramLines(metricData);
    assertThat(doubleHistogramLines).isEmpty();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            ImmutableHistogramPointData.create(
                NANOS_TS_1,
                NANOS_TS_2,
                EMPTY_ATTRIBUTES,
                10.234,
                null,
                null,
                Arrays.asList(Double.NaN, 1.2d, 3.4d, 5.6d),
                Arrays.asList(0L, 2L, 1L, 3L, 0L)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ImmutableHistogramPointData.create(
                NANOS_TS_1,
                NANOS_TS_2,
                EMPTY_ATTRIBUTES,
                10.234,
                null,
                null,
                Arrays.asList(0.1d, 1.2d, 3.4d, Double.POSITIVE_INFINITY),
                Arrays.asList(0L, 2L, 1L, 3L, 0L)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ImmutableHistogramPointData.create(
                NANOS_TS_1,
                NANOS_TS_2,
                EMPTY_ATTRIBUTES,
                10.234,
                null,
                null,
                Arrays.asList(Double.POSITIVE_INFINITY, 1.2d, 3.4d, 5.6d),
                Arrays.asList(0L, 2L, 1L, 3L, 0L)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ImmutableHistogramPointData.create(
                NANOS_TS_1,
                NANOS_TS_2,
                EMPTY_ATTRIBUTES,
                10.234,
                null,
                null,
                Arrays.asList(0.1d, 1.2d, 3.4d, Double.NEGATIVE_INFINITY),
                Arrays.asList(0L, 2L, 1L, 3L, 0L)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ImmutableHistogramPointData.create(
                NANOS_TS_1,
                NANOS_TS_2,
                EMPTY_ATTRIBUTES,
                10.234,
                null,
                null,
                Arrays.asList(Double.NEGATIVE_INFINITY, 1.2d, 3.4d, 5.6d),
                Arrays.asList(0L, 2L, 1L, 3L, 0L)));
    // Number of bounds does not match number of counts
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ImmutableHistogramPointData.create(
                NANOS_TS_1,
                NANOS_TS_2,
                EMPTY_ATTRIBUTES,
                10.234,
                null,
                null,
                Arrays.asList(1.2, 3.4),
                Arrays.asList(0L, 2L, 1L, 3L, 0L)));
  }

  @Test
  void TestGetMinFromBoundaries() {
    // A value between the first two boundaries.
    assertThat(
            Serializer.getMinFromBoundaries(
                ImmutableHistogramPointData.create(
                    NANOS_TS_1,
                    NANOS_TS_2,
                    EMPTY_ATTRIBUTES,
                    10.234,
                    null,
                    null,
                    Arrays.asList(1d, 2d, 3d, 4d, 5d),
                    Arrays.asList(0L, 1L, 0L, 3L, 0L, 4L))))
        .isCloseTo(1d, OFFSET);

    // lowest bucket has value, use the first boundary as estimation instead of -Inf
    assertThat(
            Serializer.getMinFromBoundaries(
                ImmutableHistogramPointData.create(
                    NANOS_TS_1,
                    NANOS_TS_2,
                    EMPTY_ATTRIBUTES,
                    10.234,
                    null,
                    null,
                    Arrays.asList(1d, 2d, 3d, 4d, 5d),
                    Arrays.asList(1L, 0L, 0L, 3L, 0L, 4L))))
        .isCloseTo(1d, OFFSET);

    // lowest bucket (-Inf, 1) has values, mean is lower than the lowest bucket bound and smaller
    // than the sum
    assertThat(
            Serializer.getMinFromBoundaries(
                ImmutableHistogramPointData.create(
                    NANOS_TS_1,
                    NANOS_TS_2,
                    EMPTY_ATTRIBUTES,
                    0.234,
                    null,
                    null,
                    Arrays.asList(1d, 2d, 3d, 4d, 5d),
                    Arrays.asList(3L, 0L, 0L, 0L, 0L, 0L))))
        .isCloseTo(0.234 / 3, OFFSET);

    // lowest bucket (-Inf, 0) has values, sum is lower than the lowest bucket bound
    assertThat(
            Serializer.getMinFromBoundaries(
                ImmutableHistogramPointData.create(
                    NANOS_TS_1,
                    NANOS_TS_2,
                    EMPTY_ATTRIBUTES,
                    -25.3,
                    null,
                    null,
                    Arrays.asList(0d, 5d),
                    Arrays.asList(3L, 0L, 0L))))
        .isCloseTo(-25.3, OFFSET);

    // no bucket has a value
    assertThat(
            Serializer.getMinFromBoundaries(
                ImmutableHistogramPointData.create(
                    NANOS_TS_1,
                    NANOS_TS_2,
                    EMPTY_ATTRIBUTES,
                    10.234,
                    null,
                    null,
                    Arrays.asList(1d, 2d, 3d, 4d, 5d),
                    Arrays.asList(0L, 0L, 0L, 0L, 0L, 0L))))
        .isCloseTo(10.234, OFFSET);

    // just one bucket from -Inf to Inf, calc the mean as min value.
    assertThat(
            Serializer.getMinFromBoundaries(
                ImmutableHistogramPointData.create(
                    NANOS_TS_1,
                    NANOS_TS_2,
                    EMPTY_ATTRIBUTES,
                    8.8,
                    null,
                    null,
                    Collections.emptyList(),
                    Collections.singletonList(4L))))
        .isCloseTo(2.2, Offset.offset(0.1));

    // just one bucket from -Inf to Inf, with a count of 1
    assertThat(
            Serializer.getMinFromBoundaries(
                ImmutableHistogramPointData.create(
                    NANOS_TS_1,
                    NANOS_TS_2,
                    EMPTY_ATTRIBUTES,
                    1.2,
                    null,
                    null,
                    Collections.emptyList(),
                    Collections.singletonList(1L))))
        .isCloseTo(1.2, Offset.offset(0.1));

    // only the last bucket has a value (5, +Inf)
    assertThat(
            Serializer.getMinFromBoundaries(
                ImmutableHistogramPointData.create(
                    NANOS_TS_1,
                    NANOS_TS_2,
                    EMPTY_ATTRIBUTES,
                    10.234,
                    null,
                    null,
                    Arrays.asList(1d, 2d, 3d, 4d, 5d),
                    Arrays.asList(0L, 0L, 0L, 0L, 0L, 1L))))
        .isCloseTo(5d, OFFSET);

    // skipping the test where both buckets and counts are empty, as that will throw an exception on
    // creating the ImmutableHistogramDataPoint.
  }

  @Test
  void TestGetMaxFromBoundaries() {
    // A value between the last two boundaries.
    assertThat(
            Serializer.getMaxFromBoundaries(
                ImmutableHistogramPointData.create(
                    NANOS_TS_1,
                    NANOS_TS_2,
                    EMPTY_ATTRIBUTES,
                    10.234,
                    null,
                    null,
                    Arrays.asList(1d, 2d, 3d, 4d, 5d),
                    Arrays.asList(0L, 1L, 0L, 3L, 2L, 0L))))
        .isCloseTo(5d, OFFSET);

    // last bucket has value, use the last boundary as estimation instead of Inf
    assertThat(
            Serializer.getMaxFromBoundaries(
                ImmutableHistogramPointData.create(
                    NANOS_TS_1,
                    NANOS_TS_2,
                    EMPTY_ATTRIBUTES,
                    10.234,
                    null,
                    null,
                    Arrays.asList(1d, 2d, 3d, 4d, 5d),
                    Arrays.asList(1L, 0L, 0L, 3L, 0L, 4L))))
        .isCloseTo(5d, OFFSET);

    // no bucket has a value
    assertThat(
            Serializer.getMaxFromBoundaries(
                ImmutableHistogramPointData.create(
                    NANOS_TS_1,
                    NANOS_TS_2,
                    EMPTY_ATTRIBUTES,
                    10.234,
                    null,
                    null,
                    Arrays.asList(1d, 2d, 3d, 4d, 5d),
                    Arrays.asList(0L, 0L, 0L, 0L, 0L, 0L))))
        .isCloseTo(10.234, OFFSET);

    // just one bucket from -Inf to Inf, calc the mean as max value.
    assertThat(
            Serializer.getMaxFromBoundaries(
                ImmutableHistogramPointData.create(
                    NANOS_TS_1,
                    NANOS_TS_2,
                    EMPTY_ATTRIBUTES,
                    8.8,
                    null,
                    null,
                    Collections.emptyList(),
                    Collections.singletonList(4L))))
        .isCloseTo(2.2, Offset.offset(0.1));

    // just one bucket from -Inf to Inf, with a count of 1
    assertThat(
            Serializer.getMaxFromBoundaries(
                ImmutableHistogramPointData.create(
                    NANOS_TS_1,
                    NANOS_TS_2,
                    EMPTY_ATTRIBUTES,
                    1.2,
                    null,
                    null,
                    Collections.emptyList(),
                    Collections.singletonList(1L))))
        .isCloseTo(1.2, Offset.offset(0.1));

    // only the last bucket has a value (5, +Inf)
    assertThat(
            Serializer.getMaxFromBoundaries(
                ImmutableHistogramPointData.create(
                    NANOS_TS_1,
                    NANOS_TS_2,
                    EMPTY_ATTRIBUTES,
                    10.234,
                    null,
                    null,
                    Arrays.asList(1d, 2d, 3d, 4d, 5d),
                    Arrays.asList(0L, 0L, 0L, 0L, 0L, 1L))))
        .isCloseTo(5d, OFFSET);

    // the max is greater than the sum
    assertThat(
            Serializer.getMaxFromBoundaries(
                ImmutableHistogramPointData.create(
                    NANOS_TS_1,
                    NANOS_TS_2,
                    EMPTY_ATTRIBUTES,
                    2.3,
                    null,
                    null,
                    Arrays.asList(-5d, 0d, 5d),
                    Arrays.asList(0L, 0L, 2L, 0L))))
        .isCloseTo(2.3, OFFSET);

    // skipping the test where both buckets and counts are empty, as that will throw an exception on
    // creating the ImmutableHistogramDataPoint.
  }
}
