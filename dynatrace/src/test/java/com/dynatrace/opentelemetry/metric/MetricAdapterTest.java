/**
 * Copyright 2020 Dynatrace LLC
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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dynatrace.opentelemetry.metric.mint.Datapoint;
import com.dynatrace.opentelemetry.metric.mint.Dimension;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.common.Labels;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.metrics.data.DoubleGaugeData;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.DoubleSummaryData;
import io.opentelemetry.sdk.metrics.data.DoubleSummaryPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.ValueAtPercentile;
import io.opentelemetry.sdk.resources.Resource;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MetricAdapterTest {

  @BeforeEach
  void setUp() {
    MetricAdapter.resetForTest();
  }

  @Test
  public void toDatapointsTest() {
    assertEquals(
        1,
        MetricAdapter.toDatapoints(
                MetricData.createDoubleGauge(
                    Resource.create(Attributes.empty()),
                    InstrumentationLibraryInfo.create("testlib01", "0.5.0"),
                    "test",
                    "des",
                    "ms",
                    DoubleGaugeData.create(
                        Collections.singletonList(
                            DoublePointData.create(
                                123,
                                TimeUnit.MILLISECONDS.toNanos(456),
                                Labels.of("lab01", "lab02"),
                                42)))))
            .size());

    assertTrue(
        MetricAdapter.toDatapoints(
                MetricData.createDoubleSummary(
                    Resource.create(Attributes.empty()),
                    InstrumentationLibraryInfo.create("testlib", "1.1"),
                    "test",
                    "test",
                    "ms",
                    DoubleSummaryData.create(Collections.emptyList())))
            .isEmpty());
  }

  @Test
  public void generateSummarypointTest() {
    // this test also verifies that running the metric exporter with no tags set behaves as expected
    // (i. e. no errors, and no added tags.)
    List<ValueAtPercentile> list = new ArrayList<>(2);
    list.add(ValueAtPercentile.create(0.0, 1.56));
    list.add(ValueAtPercentile.create(100.0, 345.23));

    SummaryStats.DoubleSummaryStat summaryStat =
        SummaryStats.doubleSummaryStat(1.56, 345.23, 12934, 42);

    assertEquals(
        Datapoint.create("metric_01")
            .addDimension("key01", "value01")
            .timestamp(TimeUnit.MILLISECONDS.toNanos(456))
            .value(Values.doubleGauge(summaryStat))
            .build()
            .serialize(),
        MetricAdapter.generateSummaryPoint(
                "metric_01",
                DoubleSummaryPointData.create(
                    123,
                    TimeUnit.MILLISECONDS.toNanos(456),
                    Labels.of("key01", "value01"),
                    42,
                    12934,
                    list))
            .serialize());

    assertEquals(
        "metric_01,key01=value01 gauge,min=1.56,max=345.23,sum=12934.0,count=42 456",
        MetricAdapter.generateSummaryPoint(
                "metric_01",
                DoubleSummaryPointData.create(
                    123,
                    TimeUnit.MILLISECONDS.toNanos(456),
                    Labels.of("key01", "value01"),
                    42,
                    12934,
                    list))
            .serialize());
  }

  @Test
  public void TestOneAgentDimensions() {
    OneAgentMetadataEnricher metadataEnricher = mock(OneAgentMetadataEnricher.class);
    Collection<AbstractMap.SimpleEntry<String, String>> tags = new ArrayList<>();
    tags.add(new AbstractMap.SimpleEntry<String, String>("oneagenttag", "oneagentvalue"));
    tags.add(
        new AbstractMap.SimpleEntry<String, String>("anotheroneagenttag", "anotheroneagentvalue"));
    when(metadataEnricher.getDimensionsFromOneAgentMetadata()).thenReturn(tags);

    MetricAdapter.getInstance().setTags(tags);

    List<ValueAtPercentile> list = new ArrayList<>(2);
    list.add(ValueAtPercentile.create(0.0, 1.56));
    list.add(ValueAtPercentile.create(100.0, 345.23));
    assertEquals(
        "metric_01,oneagenttag=oneagentvalue,key01=value01,anotheroneagenttag=anotheroneagentvalue gauge,min=1.56,max=345.23,sum=12934.0,count=42 456",
        MetricAdapter.generateSummaryPoint(
                "metric_01",
                DoubleSummaryPointData.create(
                    123,
                    TimeUnit.MILLISECONDS.toNanos(456),
                    Labels.of("key01", "value01"),
                    42,
                    12934,
                    list))
            .serialize());
  }

  @Test
  public void TestAddOneAgentDimensionsEmptyList() {
    OneAgentMetadataEnricher metadataEnricher = mock(OneAgentMetadataEnricher.class);
    Collection<AbstractMap.SimpleEntry<String, String>> tags = new ArrayList<>();
    when(metadataEnricher.getDimensionsFromOneAgentMetadata()).thenReturn(tags);

    MetricAdapter.getInstance().setTags(tags);

    List<ValueAtPercentile> list = new ArrayList<>(2);
    list.add(ValueAtPercentile.create(0.0, 1.56));
    list.add(ValueAtPercentile.create(100.0, 345.23));
    assertEquals(
        "metric_01,key01=value01 gauge,min=1.56,max=345.23,sum=12934.0,count=42 456",
        MetricAdapter.generateSummaryPoint(
                "metric_01",
                DoubleSummaryPointData.create(
                    123,
                    TimeUnit.MILLISECONDS.toNanos(456),
                    Labels.of("key01", "value01"),
                    42,
                    12934,
                    list))
            .serialize());
  }

  @Test
  public void TestOneAgentDimensionsOverwrite() {
    Collection<AbstractMap.SimpleEntry<String, String>> tags =
        Arrays.asList(
            new AbstractMap.SimpleEntry<>("oneagenttag", "oneagentvalue"),
            new AbstractMap.SimpleEntry<>("oneagenttag2", "oneagentvalue2"));
    MetricAdapter.getInstance().setTags(tags);

    List<ValueAtPercentile> list = new ArrayList<>(2);
    list.add(ValueAtPercentile.create(0.0, 1.56));
    list.add(ValueAtPercentile.create(100.0, 345.23));
    assertEquals(
        "metric_01,oneagenttag=oneagentvalue,oneagenttag2=oneagentvalue2 gauge,min=1.56,max=345.23,sum=12934.0,count=42 456",
        MetricAdapter.generateSummaryPoint(
                "metric_01",
                DoubleSummaryPointData.create(
                    123,
                    TimeUnit.MILLISECONDS.toNanos(456),
                    // these will be overritten by the tags set in the singleton.
                    Labels.of("oneAgentTag", "someValue", "oneAgentTag2", "someOtherValue"),
                    42,
                    12934,
                    list))
            .serialize());
  }

  @Test
  public void Test_getUniqueCombinedDimensions_Valid() {
    Labels labels = Labels.of("dim1", "dv1", "dim2", "dv2");
    Collection<AbstractMap.SimpleEntry<String, String>> tags =
        Arrays.asList(
            new AbstractMap.SimpleEntry<>("tag1", "tv1"),
            new AbstractMap.SimpleEntry<>("tag2", "tv2"));
    MetricAdapter.getInstance().setTags(tags);

    List<Dimension> expected =
        Arrays.asList(
            Dimension.create("dim1", "dv1"),
            Dimension.create("dim2", "dv2"),
            Dimension.create("tag1", "tv1"),
            Dimension.create("tag2", "tv2"));
    // the array list wrap is so the list is sortable as it is otherwise unmodifiable.
    List<Dimension> got = new ArrayList<>(MetricAdapter.getUniqueCombinedDimensions(labels));

    expected.sort(Comparator.comparing(Dimension::getKey));
    got.sort(Comparator.comparing(Dimension::getKey));

    assertEquals(expected, got);
    assertNotSame(expected, got);
  }

  @Test
  public void Test_getUniqueCombinedDimensions_Overwriting() {
    Labels labels = Labels.of("dim1", "dv1", "dim2", "dv2");
    Collection<AbstractMap.SimpleEntry<String, String>> tags =
        Arrays.asList(
            new AbstractMap.SimpleEntry<>("dim1", "tagValue1"),
            new AbstractMap.SimpleEntry<>("tag2", "tagValue2"));
    MetricAdapter.getInstance().setTags(tags);

    List<Dimension> expected =
        Arrays.asList(
            Dimension.create("dim1", "tagValue1"),
            Dimension.create("dim2", "dv2"),
            Dimension.create("tag2", "tagValue2"));

    List<Dimension> got = new ArrayList<>(MetricAdapter.getUniqueCombinedDimensions(labels));
    expected.sort(Comparator.comparing(Dimension::getKey));
    got.sort(Comparator.comparing(Dimension::getKey));

    assertEquals(expected, got);
    assertNotSame(expected, got);
  }

  @Test
  public void Test_getUniqueCombinedDimensions_OverwritingSameKey() {
    Labels labels = Labels.of("dim1", "dv1", "dim1", "dv2");
    Collection<AbstractMap.SimpleEntry<String, String>> tags =
        Arrays.asList(
            new AbstractMap.SimpleEntry<>("dim1", "tagValue1"),
            new AbstractMap.SimpleEntry<>("dim1", "tagValue2"));
    MetricAdapter.getInstance().setTags(tags);

    List<Dimension> expected = Arrays.asList(Dimension.create("dim1", "tagValue2"));

    List<Dimension> got = new ArrayList<>(MetricAdapter.getUniqueCombinedDimensions(labels));
    expected.sort(Comparator.comparing(Dimension::getKey));
    got.sort(Comparator.comparing(Dimension::getKey));

    assertEquals(1, got.size());
    assertEquals(expected, got);
    assertNotSame(expected, got);
  }
}
