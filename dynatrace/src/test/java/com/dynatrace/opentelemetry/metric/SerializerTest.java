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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import com.dynatrace.metric.util.Dimension;
import com.dynatrace.metric.util.DimensionList;
import com.dynatrace.metric.util.MetricBuilderFactory;
import io.opentelemetry.api.metrics.common.Labels;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.metrics.data.*;
import io.opentelemetry.sdk.resources.Resource;
import java.util.*;
import org.junit.jupiter.api.Test;

class SerializerTest {
  private static final Serializer serializer =
      new Serializer(MetricBuilderFactory.builder().build());

  @Test
  void fromLabels() {
    Labels labels = Labels.of("label1", "value1", "label2", "value2");
    DimensionList expected =
        DimensionList.create(
            Dimension.create("label1", "value1"), Dimension.create("label2", "value2"));
    DimensionList actual = Serializer.fromLabels(labels);

    assertEquals(expected.getDimensions(), actual.getDimensions());
    assertNotSame(expected, actual);
  }

  @Test
  void fromLabelsEmpty() {
    Labels labels = Labels.empty();
    DimensionList expected = DimensionList.create();
    DimensionList actual = Serializer.fromLabels(labels);

    assertEquals(expected.getDimensions(), actual.getDimensions());
    assertNotSame(expected, actual);
  }

  @Test
  void fromLabelsNormalize() {
    Labels labels = Labels.of("~~!123", "test", "!!test2", "test2");
    DimensionList expected = DimensionList.create(Dimension.create("test2", "test2"));
    DimensionList actual = Serializer.fromLabels(labels);

    assertEquals(expected.getDimensions(), actual.getDimensions());
    assertNotSame(expected, actual);
  }

  @Test
  void createLongSumLinesCumulative() {
    Collection<LongPointData> longPointDataCollection =
        new ArrayList<LongPointData>() {
          {
            add(
                LongPointData.create(
                    1619687639000000000L, 1619687659000000000L, Labels.empty(), 123L));
            add(
                LongPointData.create(
                    1619687639000000000L, 1619687659000000000L, Labels.empty(), 321L));
          }
        };
    LongSumData longSumData =
        LongSumData.create(true, AggregationTemporality.CUMULATIVE, longPointDataCollection);
    MetricData metricData =
        MetricData.createLongSum(
            Resource.getDefault(),
            InstrumentationLibraryInfo.empty(),
            "longSumData",
            "",
            "",
            longSumData);

    List<String> longSumLines = serializer.createLongSumLines(metricData, false);
    assertThat(longSumLines).hasSize(2);
    assertThat(longSumLines.get(0)).isEqualTo("longSumData count,123 1619687659000");
    assertThat(longSumLines.get(1)).isEqualTo("longSumData count,321 1619687659000");
  }

  @Test
  void createLongSumLinesCumulativeNoTimestamp() {
    Collection<LongPointData> longPointDataCollection =
        new ArrayList<LongPointData>() {
          {
            add(LongPointData.create(0L, 0L, Labels.empty(), 123L));
          }
        };
    LongSumData longSumData =
        LongSumData.create(true, AggregationTemporality.CUMULATIVE, longPointDataCollection);
    MetricData metricData =
        MetricData.createLongSum(
            Resource.getDefault(),
            InstrumentationLibraryInfo.empty(),
            "longSumData",
            "",
            "",
            longSumData);

    List<String> longSumLines = serializer.createLongSumLines(metricData, false);
    assertThat(longSumLines).hasSize(1);
    assertThat(longSumLines.get(0)).isEqualTo("longSumData count,123");
  }

  @Test
  void createLongSumLinesDelta() {
    Collection<LongPointData> longPointDataCollection =
        new ArrayList<LongPointData>() {
          {
            add(
                LongPointData.create(
                    1619687639000000000L, 1619687659000000000L, Labels.empty(), 123L));
            add(
                LongPointData.create(
                    1619687639000000000L, 1619687659000000000L, Labels.empty(), 321L));
          }
        };
    LongSumData longSumData =
        LongSumData.create(true, AggregationTemporality.DELTA, longPointDataCollection);
    MetricData metricData =
        MetricData.createLongSum(
            Resource.getDefault(),
            InstrumentationLibraryInfo.empty(),
            "longSumData",
            "",
            "",
            longSumData);

    List<String> longSumLines = serializer.createLongSumLines(metricData, true);
    assertThat(longSumLines).hasSize(2);
    assertThat(longSumLines.get(0)).isEqualTo("longSumData count,delta=123 1619687659000");
    assertThat(longSumLines.get(1)).isEqualTo("longSumData count,delta=321 1619687659000");
  }

  @Test
  void createLongSumLinesTimeDeltaNoTimestamp() {
    Collection<LongPointData> longPointDataCollection =
        new ArrayList<LongPointData>() {
          {
            add(LongPointData.create(0L, 0L, Labels.empty(), 123L));
          }
        };
    LongSumData longSumData =
        LongSumData.create(true, AggregationTemporality.DELTA, longPointDataCollection);
    MetricData metricData =
        MetricData.createLongSum(
            Resource.getDefault(),
            InstrumentationLibraryInfo.empty(),
            "longSumData",
            "",
            "",
            longSumData);

    List<String> longSumLines = serializer.createLongSumLines(metricData, true);
    assertThat(longSumLines).hasSize(1);
    assertThat(longSumLines.get(0)).isEqualTo("longSumData count,delta=123");
  }

  @Test
  void createLongGaugeLines() {
    Collection<LongPointData> longPointDataCollection =
        new ArrayList<LongPointData>() {
          {
            add(
                LongPointData.create(
                    1619687639000000000L, 1619687659000000000L, Labels.empty(), 123L));
            add(
                LongPointData.create(
                    1619687639000000000L, 1619687659000000000L, Labels.empty(), 321L));
          }
        };
    LongGaugeData longGaugeData = LongGaugeData.create(longPointDataCollection);
    MetricData metricData =
        MetricData.createLongGauge(
            Resource.getDefault(),
            InstrumentationLibraryInfo.empty(),
            "longGaugeData",
            "",
            "",
            longGaugeData);

    List<String> longGaugeLines = serializer.createLongGaugeLines(metricData);
    assertThat(longGaugeLines).hasSize(2);
    assertThat(longGaugeLines.get(0)).isEqualTo("longGaugeData gauge,123 1619687659000");
    assertThat(longGaugeLines.get(1)).isEqualTo("longGaugeData gauge,321 1619687659000");
  }

  @Test
  void createDoubleGaugeLines() {
    Collection<DoublePointData> doublePointDataCollection =
        new ArrayList<DoublePointData>() {
          {
            add(
                DoublePointData.create(
                    1619687639000000000L, 1619687659000000000L, Labels.empty(), 123.456d));
            add(
                DoublePointData.create(
                    1619687639000000000L, 1619687659000000000L, Labels.empty(), 321.456d));
          }
        };
    DoubleGaugeData doubleGaugeData = DoubleGaugeData.create(doublePointDataCollection);
    MetricData metricData =
        MetricData.createDoubleGauge(
            Resource.getDefault(),
            InstrumentationLibraryInfo.empty(),
            "doubleGaugeData",
            "",
            "",
            doubleGaugeData);

    List<String> doubleGaugeLines = serializer.createDoubleGaugeLines(metricData);
    assertThat(doubleGaugeLines).hasSize(2);
    assertThat(doubleGaugeLines.get(0)).isEqualTo("doubleGaugeData gauge,123.456 1619687659000");
    assertThat(doubleGaugeLines.get(1)).isEqualTo("doubleGaugeData gauge,321.456 1619687659000");
  }

  @Test
  void createInvalidDoubleGaugeLines() {
    Collection<DoublePointData> doublePointDataCollection =
        new ArrayList<DoublePointData>() {
          {
            add(
                DoublePointData.create(
                    1619687639000000000L, 1619687659000000000L, Labels.empty(), Double.NaN));
            add(
                DoublePointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Labels.empty(),
                    Double.POSITIVE_INFINITY));
            add(
                DoublePointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Labels.empty(),
                    Double.NEGATIVE_INFINITY));
          }
        };
    DoubleGaugeData doubleGaugeData = DoubleGaugeData.create(doublePointDataCollection);
    MetricData metricData =
        MetricData.createDoubleGauge(
            Resource.getDefault(),
            InstrumentationLibraryInfo.empty(),
            "doubleGaugeData",
            "",
            "",
            doubleGaugeData);

    List<String> doubleGaugeLines = serializer.createDoubleGaugeLines(metricData);
    assertThat(doubleGaugeLines).isEmpty();
  }

  @Test
  void createDoubleSumLinesCumulative() {
    Collection<DoublePointData> doublePointDataCollection =
        new ArrayList<DoublePointData>() {
          {
            add(
                DoublePointData.create(
                    1619687639000000000L, 1619687659000000000L, Labels.empty(), 123.456d));
            add(
                DoublePointData.create(
                    1619687639000000000L, 1619687659000000000L, Labels.empty(), 321.456d));
          }
        };
    DoubleSumData doubleSumData =
        DoubleSumData.create(true, AggregationTemporality.CUMULATIVE, doublePointDataCollection);
    MetricData metricData =
        MetricData.createDoubleSum(
            Resource.getDefault(),
            InstrumentationLibraryInfo.empty(),
            "doubleSumData",
            "",
            "",
            doubleSumData);

    List<String> doubleSumLines = serializer.createDoubleSumLines(metricData, false);
    assertThat(doubleSumLines).hasSize(2);
    assertThat(doubleSumLines.get(0)).isEqualTo("doubleSumData count,123.456 1619687659000");
    assertThat(doubleSumLines.get(1)).isEqualTo("doubleSumData count,321.456 1619687659000");
  }

  @Test
  void createInvalidDoubleSumLinesCumulative() {
    Collection<DoublePointData> doublePointDataCollection =
        new ArrayList<DoublePointData>() {
          {
            add(
                DoublePointData.create(
                    1619687639000000000L, 1619687659000000000L, Labels.empty(), Double.NaN));
            add(
                DoublePointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Labels.empty(),
                    Double.POSITIVE_INFINITY));
            add(
                DoublePointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Labels.empty(),
                    Double.NEGATIVE_INFINITY));
          }
        };
    DoubleSumData doubleSumData =
        DoubleSumData.create(true, AggregationTemporality.CUMULATIVE, doublePointDataCollection);
    MetricData metricData =
        MetricData.createDoubleSum(
            Resource.getDefault(),
            InstrumentationLibraryInfo.empty(),
            "doubleSumData",
            "",
            "",
            doubleSumData);

    List<String> doubleSumLines = serializer.createDoubleSumLines(metricData, false);
    assertThat(doubleSumLines).isEmpty();
  }

  @Test
  void createDoubleSumLinesDelta() {
    Collection<DoublePointData> doublePointDataCollection =
        new ArrayList<DoublePointData>() {
          {
            add(
                DoublePointData.create(
                    1619687639000000000L, 1619687659000000000L, Labels.empty(), 123.456d));
            add(
                DoublePointData.create(
                    1619687639000000000L, 1619687659000000000L, Labels.empty(), 321.456d));
          }
        };
    DoubleSumData doubleSumData =
        DoubleSumData.create(true, AggregationTemporality.DELTA, doublePointDataCollection);
    MetricData metricData =
        MetricData.createDoubleSum(
            Resource.getDefault(),
            InstrumentationLibraryInfo.empty(),
            "doubleSumData",
            "",
            "",
            doubleSumData);

    List<String> doubleSumLines = serializer.createDoubleSumLines(metricData, true);
    assertThat(doubleSumLines).hasSize(2);
    assertThat(doubleSumLines.get(0)).isEqualTo("doubleSumData count,delta=123.456 1619687659000");
    assertThat(doubleSumLines.get(1)).isEqualTo("doubleSumData count,delta=321.456 1619687659000");
  }

  @Test
  void createDoubleSummaryLines() {
    Collection<DoubleSummaryPointData> doubleSummaryPointDataCollection =
        new ArrayList<DoubleSummaryPointData>() {
          {
            add(
                DoubleSummaryPointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Labels.empty(),
                    7,
                    700.70d,
                    Arrays.asList(
                        ValueAtPercentile.create(0.0, 0.1),
                        ValueAtPercentile.create(100.0, 100.1))));
            add(
                DoubleSummaryPointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Labels.empty(),
                    3,
                    202.66d,
                    Arrays.asList(
                        ValueAtPercentile.create(0.0, 0.22),
                        ValueAtPercentile.create(100.0, 123.45))));
          }
        };

    DoubleSummaryData doubleSummaryData =
        DoubleSummaryData.create(doubleSummaryPointDataCollection);
    MetricData metricData =
        MetricData.createDoubleSummary(
            Resource.getDefault(),
            InstrumentationLibraryInfo.empty(),
            "doubleSummary",
            "",
            "",
            doubleSummaryData);

    List<String> doubleSummaryLines = serializer.createDoubleSummaryLines(metricData);
    assertThat(doubleSummaryLines).hasSize(2);
    assertThat(doubleSummaryLines.get(0))
        .isEqualTo("doubleSummary gauge,min=0.1,max=100.1,sum=700.7,count=7 1619687659000");
    assertThat(doubleSummaryLines.get(1))
        .isEqualTo("doubleSummary gauge,min=0.22,max=123.45,sum=202.66,count=3 1619687659000");
  }

  @Test
  void createInvalidDoubleSummaryLines() {
    Collection<DoubleSummaryPointData> doubleSummaryPointDataCollection =
        new ArrayList<DoubleSummaryPointData>() {
          {
            // NaN
            add(
                DoubleSummaryPointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Labels.empty(),
                    3,
                    Double.NaN,
                    Arrays.asList(
                        ValueAtPercentile.create(0.0, 0.22),
                        ValueAtPercentile.create(100.0, 123.45))));
            // +Infinity
            add(
                DoubleSummaryPointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Labels.empty(),
                    3,
                    Double.POSITIVE_INFINITY,
                    Arrays.asList(
                        ValueAtPercentile.create(0.0, 0.22),
                        ValueAtPercentile.create(100.0, 123.45))));
            // -Infinity
            add(
                DoubleSummaryPointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Labels.empty(),
                    3,
                    Double.NEGATIVE_INFINITY,
                    Arrays.asList(
                        ValueAtPercentile.create(0.0, 0.22),
                        ValueAtPercentile.create(100.0, 123.45))));
            // Will be dropped since no min and max are set and are, by default, assumed to be NaN.
            add(
                DoubleSummaryPointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Labels.empty(),
                    0,
                    660.66d,
                    Collections.emptyList()));
          }
        };

    DoubleSummaryData doubleSummaryData =
        DoubleSummaryData.create(doubleSummaryPointDataCollection);
    MetricData metricData =
        MetricData.createDoubleSummary(
            Resource.getDefault(),
            InstrumentationLibraryInfo.empty(),
            "doubleSummary",
            "",
            "",
            doubleSummaryData);

    List<String> doubleSummaryLines = serializer.createDoubleSummaryLines(metricData);
    assertThat(doubleSummaryLines).isEmpty();
  }

  @Test
  void createDoubleHistogramLines() {
    Collection<DoubleHistogramPointData> doubleHistogramPointDataCollection =
        new ArrayList<DoubleHistogramPointData>() {
          {
            add(
                DoubleHistogramPointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Labels.empty(),
                    10.123d,
                    Arrays.asList(0.1d, 1.2d, 3.4d, 5.6d),
                    Arrays.asList(0L, 2L, 1L, 3L, 0L)));
          }
        };

    DoubleHistogramData doubleHistogramData =
        DoubleHistogramData.create(
            AggregationTemporality.CUMULATIVE, doubleHistogramPointDataCollection);
    MetricData metricData =
        MetricData.createDoubleHistogram(
            Resource.getDefault(),
            InstrumentationLibraryInfo.empty(),
            "doubleHistogram",
            "",
            "",
            doubleHistogramData);

    List<String> doubleHistogramLines = serializer.createDoubleHistogramLines(metricData);
    assertThat(doubleHistogramLines).hasSize(1);
    assertThat(doubleHistogramLines.get(0))
        .isEqualTo("doubleHistogram gauge,min=0.1,max=5.6,sum=10.123,count=6 1619687659000");
  }

  @Test
  void createInvalidDoubleHistogramLines() {
    Collection<DoubleHistogramPointData> doubleHistogramPointDataCollection =
        new ArrayList<DoubleHistogramPointData>() {
          {
            add(
                DoubleHistogramPointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Labels.empty(),
                    Double.NaN,
                    Arrays.asList(0.1d, 1.2d, 3.4d, 5.6d),
                    Arrays.asList(0L, 2L, 1L, 3L, 0L)));
            add(
                DoubleHistogramPointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Labels.empty(),
                    Double.NEGATIVE_INFINITY,
                    Arrays.asList(0.1d, 1.2d, 3.4d, 5.6d),
                    Arrays.asList(0L, 2L, 1L, 3L, 0L)));
            add(
                DoubleHistogramPointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Labels.empty(),
                    Double.POSITIVE_INFINITY,
                    Arrays.asList(0.1d, 1.2d, 3.4d, 5.6d),
                    Arrays.asList(0L, 2L, 1L, 3L, 0L)));
            add(
                DoubleHistogramPointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Labels.empty(),
                    10.234,
                    Arrays.asList(0.1d, 1.2d, 3.4d, Double.NaN),
                    Arrays.asList(0L, 2L, 1L, 3L, 0L)));
          }
        };

    DoubleHistogramData doubleHistogramData =
        DoubleHistogramData.create(
            AggregationTemporality.CUMULATIVE, doubleHistogramPointDataCollection);
    MetricData metricData =
        MetricData.createDoubleHistogram(
            Resource.getDefault(),
            InstrumentationLibraryInfo.empty(),
            "doubleHistogram",
            "",
            "",
            doubleHistogramData);

    List<String> doubleHistogramLines = serializer.createDoubleHistogramLines(metricData);
    assertThat(doubleHistogramLines).isEmpty();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            DoubleHistogramPointData.create(
                1619687639000000000L,
                1619687659000000000L,
                Labels.empty(),
                10.234,
                Arrays.asList(Double.NaN, 1.2d, 3.4d, 5.6d),
                Arrays.asList(0L, 2L, 1L, 3L, 0L)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DoubleHistogramPointData.create(
                1619687639000000000L,
                1619687659000000000L,
                Labels.empty(),
                10.234,
                Arrays.asList(0.1d, 1.2d, 3.4d, Double.POSITIVE_INFINITY),
                Arrays.asList(0L, 2L, 1L, 3L, 0L)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DoubleHistogramPointData.create(
                1619687639000000000L,
                1619687659000000000L,
                Labels.empty(),
                10.234,
                Arrays.asList(Double.POSITIVE_INFINITY, 1.2d, 3.4d, 5.6d),
                Arrays.asList(0L, 2L, 1L, 3L, 0L)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DoubleHistogramPointData.create(
                1619687639000000000L,
                1619687659000000000L,
                Labels.empty(),
                10.234,
                Arrays.asList(0.1d, 1.2d, 3.4d, Double.NEGATIVE_INFINITY),
                Arrays.asList(0L, 2L, 1L, 3L, 0L)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            DoubleHistogramPointData.create(
                1619687639000000000L,
                1619687659000000000L,
                Labels.empty(),
                10.234,
                Arrays.asList(Double.NEGATIVE_INFINITY, 1.2d, 3.4d, 5.6d),
                Arrays.asList(0L, 2L, 1L, 3L, 0L)));
  }
}
