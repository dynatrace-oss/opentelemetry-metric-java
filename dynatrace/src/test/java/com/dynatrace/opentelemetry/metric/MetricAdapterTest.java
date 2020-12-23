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

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dynatrace.opentelemetry.metric.mint.Datapoint;
import com.dynatrace.opentelemetry.metric.mint.Dimension;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.Labels;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MetricAdapterTest {

  @Test
  public void generateDatapointTest() throws DynatraceExporterException {
    assertEquals(
        Datapoint.create("keyname_01")
            .timestamp(4000000)
            .addDimension("dim01", "value01")
            .value(Values.longCount(5, false))
            .build()
            .serialize(),
        MetricAdapter.generateDatapoint(
                "keyname_01",
                Collections.singletonList(Dimension.create("dim01", "value01")),
                MetricData.LongPoint.create(123, 4560000, Labels.empty(), 5),
                MetricData.Type.LONG_SUM)
            .serialize());

    assertEquals(
        "keyname_01,dim01=value01 count,5 4",
        MetricAdapter.generateDatapoint(
                "keyname_01",
                Collections.singletonList(Dimension.create("dim01", "value01")),
                MetricData.LongPoint.create(123, 4560000, Labels.empty(), 5),
                MetricData.Type.LONG_SUM)
            .serialize());

    assertEquals(
        Datapoint.create("keyname_01")
            .timestamp(4000000)
            .addDimension("dim01", "value01")
            .value(Values.doubleCount(5.0, false))
            .build()
            .serialize(),
        MetricAdapter.generateDatapoint(
                "keyname_01",
                Collections.singletonList(Dimension.create("dim01", "value01")),
                MetricData.DoublePoint.create(123, 4560000, Labels.empty(), 5.0),
                MetricData.Type.DOUBLE_SUM)
            .serialize());

    assertEquals(
        "keyname_02,dim02=value02 count,194.0 4",
        MetricAdapter.generateDatapoint(
                "keyname_02",
                Collections.singletonList(Dimension.create("dim02", "value02")),
                MetricData.DoublePoint.create(123, 4560000, Labels.empty(), 194.0),
                MetricData.Type.DOUBLE_SUM)
            .serialize());
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
                    MetricData.DoubleGaugeData.create(
                        Collections.singletonList(
                            MetricData.DoublePoint.create(
                                123, 456, Labels.of("lab01", "lab02"), 42)))))
            .size());

    Assertions.assertTrue(
        MetricAdapter.toDatapoints(
                MetricData.createDoubleSummary(
                    Resource.create(Attributes.empty()),
                    InstrumentationLibraryInfo.create("testlib", "1.1"),
                    "test",
                    "test",
                    "ms",
                    MetricData.DoubleSummaryData.create(Collections.emptyList())))
            .isEmpty());
  }

  @Test
  public void generateSummarypointTest() {

    List<MetricData.ValueAtPercentile> list = new ArrayList<>(2);
    list.add(MetricData.ValueAtPercentile.create(0.0, 1.56));
    list.add(MetricData.ValueAtPercentile.create(100.0, 345.23));

    SummaryStats.DoubleSummaryStat summaryStat =
        SummaryStats.doubleSummaryStat(1.56, 345.23, 12934, 42);

    assertEquals(
        Datapoint.create("metric_01")
            .addDimension("key01", "value01")
            .timestamp(4000000)
            .value(Values.doubleGauge(summaryStat))
            .build()
            .serialize(),
        MetricAdapter.generateSummaryPoint(
                "metric_01",
                MetricData.DoubleSummaryPoint.create(123, 4560000, Labels.empty(), 42, 12934, list),
                Collections.singletonList(Dimension.create("key01", "value01")))
            .serialize());

    assertEquals(
        "metric_01,key01=value01 gauge,min=1.56,max=345.23,sum=12934.0,count=42 456",
        MetricAdapter.generateSummaryPoint(
                "metric_01",
                MetricData.DoubleSummaryPoint.create(
                    123, 456000000, Labels.empty(), 42, 12934, list),
                Collections.singletonList(Dimension.create("key01", "value01")))
            .serialize());
  }
}
