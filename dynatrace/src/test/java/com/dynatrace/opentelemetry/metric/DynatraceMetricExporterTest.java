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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.common.InstrumentType;
import io.opentelemetry.sdk.metrics.data.*;
import io.opentelemetry.sdk.metrics.view.Aggregation;
import io.opentelemetry.sdk.metrics.view.InstrumentSelector;
import io.opentelemetry.sdk.metrics.view.View;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.time.TestClock;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;

class DynatraceMetricExporterTest {

  public static MetricData generateMetricData() {
    return generateMetricDataWithAttributes(Attributes.empty());
  }

  private final TestClock testClock = TestClock.create();
  private static final long SECOND_NANOS = 1_000_000_000;

  public static MetricData generateMetricDataWithAttributes(Attributes attributes) {
    return MetricData.createDoubleSum(
        Resource.create(Attributes.builder().build()),
        InstrumentationLibraryInfo.empty(),
        "name",
        "desc",
        "",
        DoubleSumData.create(
            true,
            AggregationTemporality.DELTA,
            Collections.singleton(
                DoublePointData.create(
                    1619687639000000000L, 1619687659000000000L, attributes, 194.0))));
  }

  @Test
  public void testExport() throws IOException {
    MetricData md = generateMetricData();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayInputStream bis =
        new ByteArrayInputStream(
            "{\n\"linesOk\": 1,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

    HttpURLConnection connection = setUpMockConnection(202, bos, bis);

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();

    CompletableResultCode result = metricExporter.export(Collections.singleton(md), connection);

    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    assertEquals(
        "name,dt.metrics.source=opentelemetry count,delta=194.0 1619687659000", bos.toString());
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }

  @Test
  public void testFailedExport() throws IOException {
    MetricData md = generateMetricData();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();

    HttpURLConnection connection = setUpMockConnection(400, bos, null);

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();
    CompletableResultCode result = metricExporter.export(Collections.singleton(md), connection);

    assertEquals(CompletableResultCode.ofFailure(), result);
  }

  @Test
  public void testAddPrefix() throws IOException {
    MetricData md = generateMetricData();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayInputStream bis =
        new ByteArrayInputStream(
            "{\n\"linesOk\": 1,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

    HttpURLConnection connection = setUpMockConnection(202, bos, bis);

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .setPrefix("prefix")
            .build();

    CompletableResultCode result = metricExporter.export(Collections.singleton(md), connection);

    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    assertEquals(
        "prefix.name,dt.metrics.source=opentelemetry count,delta=194.0 1619687659000",
        bos.toString());
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }

  @Test
  public void addDefaultDimensions() throws IOException {
    MetricData md = generateMetricData();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayInputStream bis =
        new ByteArrayInputStream(
            "{\n\"linesOk\": 1,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

    HttpURLConnection connection = setUpMockConnection(202, bos, bis);

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .setDefaultDimensions(Attributes.of(AttributeKey.stringKey("default"), "value"))
            .build();

    CompletableResultCode result = metricExporter.export(Collections.singleton(md), connection);

    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    assertEquals(
        "name,default=value,dt.metrics.source=opentelemetry count,delta=194.0 1619687659000",
        bos.toString());
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }

  @Test
  public void testWithAttributes() throws IOException {
    Attributes attributes =
        Attributes.builder().put("attr1", "val1").put("attr2", "val2").build();
    MetricData md = generateMetricDataWithAttributes(attributes);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayInputStream bis =
        new ByteArrayInputStream(
            "{\n\"linesOk\": 1,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

    HttpURLConnection connection = setUpMockConnection(202, bos, bis);

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();

    CompletableResultCode result = metricExporter.export(Collections.singleton(md), connection);

    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    assertEquals(
        "name,dt.metrics.source=opentelemetry,attr1=val1,attr2=val2 count,delta=194.0 1619687659000",
        bos.toString());
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }

  @Test
  void testLongSumLinesCumulative() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayInputStream bis =
        new ByteArrayInputStream(
            "{\n\"linesOk\": 2,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

    HttpURLConnection connection = setUpMockConnection(202, bos, bis);

    Collection<LongPointData> longPointDataCollection =
        new ArrayList<LongPointData>() {
          {
            add(
                LongPointData.create(
                    1619687639000000000L, 1619687659000000000L, Attributes.empty(), 123L));
            add(
                LongPointData.create(
                    1619687639000000000L, 1619687659000000000L, Attributes.empty(), 321L));
            add(LongPointData.create(0L, 0L, Attributes.empty(), 456L));
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

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();

    CompletableResultCode result =
        metricExporter.export(Collections.singleton(metricData), connection);

    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    assertEquals(
        "longSumData,dt.metrics.source=opentelemetry count,delta=198 1619687659000\n"
            + "longSumData,dt.metrics.source=opentelemetry count,delta=135",
        bos.toString());
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }

  @Test
  void testLongSumLinesDelta() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayInputStream bis =
        new ByteArrayInputStream(
            "{\n\"linesOk\": 3,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

    HttpURLConnection connection = setUpMockConnection(202, bos, bis);

    Collection<LongPointData> longPointDataCollection =
        new ArrayList<LongPointData>() {
          {
            add(
                LongPointData.create(
                    1619687639000000000L, 1619687659000000000L, Attributes.empty(), 123L));
            add(
                LongPointData.create(
                    1619687639000000000L, 1619687659000000000L, Attributes.empty(), 321L));
            add(LongPointData.create(0L, 0L, Attributes.empty(), 456L));
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

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();

    CompletableResultCode result =
        metricExporter.export(Collections.singleton(metricData), connection);

    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    assertEquals(
        "longSumData,dt.metrics.source=opentelemetry count,delta=123 1619687659000\n"
            + "longSumData,dt.metrics.source=opentelemetry count,delta=321 1619687659000\n"
            + "longSumData,dt.metrics.source=opentelemetry count,delta=456",
        bos.toString());
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }

  @Test
  void testDoubleSumLinesCumulative() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayInputStream bis =
        new ByteArrayInputStream(
            "{\n\"linesOk\": 2,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

    HttpURLConnection connection = setUpMockConnection(202, bos, bis);

    Collection<DoublePointData> doublePointDataCollection =
        new ArrayList<DoublePointData>() {
          {
            add(
                DoublePointData.create(
                    1619687639000000000L, 1619687659000000000L, Attributes.empty(), 100.3));
            add(
                DoublePointData.create(
                    1619687639000000000L, 1619687659000000000L, Attributes.empty(), 300.6));
            add(DoublePointData.create(0L, 0L, Attributes.empty(), 500.8));
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

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();

    CompletableResultCode result =
        metricExporter.export(Collections.singleton(metricData), connection);

    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    assertEquals(
        "doubleSumData,dt.metrics.source=opentelemetry count,delta=200.3 1619687659000\n"
            + "doubleSumData,dt.metrics.source=opentelemetry count,delta=200.2",
        bos.toString());
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }

  @Test
  void testDoubleSumLinesDelta() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayInputStream bis =
        new ByteArrayInputStream(
            "{\n\"linesOk\": 3,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

    HttpURLConnection connection = setUpMockConnection(202, bos, bis);

    Collection<DoublePointData> doublePointDataCollection =
        new ArrayList<DoublePointData>() {
          {
            add(
                DoublePointData.create(
                    1619687639000000000L, 1619687659000000000L, Attributes.empty(), 123.456d));
            add(
                DoublePointData.create(
                    1619687639000000000L, 1619687659000000000L, Attributes.empty(), 321.456d));
            add(DoublePointData.create(0L, 0L, Attributes.empty(), 654.321d));
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

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();

    CompletableResultCode result =
        metricExporter.export(Collections.singleton(metricData), connection);

    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    assertEquals(
        "doubleSumData,dt.metrics.source=opentelemetry count,delta=123.456 1619687659000\n"
            + "doubleSumData,dt.metrics.source=opentelemetry count,delta=321.456 1619687659000\n"
            + "doubleSumData,dt.metrics.source=opentelemetry count,delta=654.321",
        bos.toString());
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }

  @Test
  void testLongGaugeLines() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayInputStream bis =
        new ByteArrayInputStream(
            "{\n\"linesOk\": 3,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

    HttpURLConnection connection = setUpMockConnection(202, bos, bis);

    Collection<LongPointData> longPointDataCollection =
        new ArrayList<LongPointData>() {
          {
            add(
                LongPointData.create(
                    1619687639000000000L, 1619687659000000000L, Attributes.empty(), 123L));
            add(
                LongPointData.create(
                    1619687639000000000L, 1619687659000000000L, Attributes.empty(), 321L));
            add(LongPointData.create(0L, 0L, Attributes.empty(), 456L));
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

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();

    CompletableResultCode result =
        metricExporter.export(Collections.singleton(metricData), connection);

    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    assertEquals(
        "longGaugeData,dt.metrics.source=opentelemetry gauge,123 1619687659000\n"
            + "longGaugeData,dt.metrics.source=opentelemetry gauge,321 1619687659000\n"
            + "longGaugeData,dt.metrics.source=opentelemetry gauge,456",
        bos.toString());
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }

  @Test
  void testDoubleGaugeLines() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayInputStream bis =
        new ByteArrayInputStream(
            "{\n\"linesOk\": 3,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

    HttpURLConnection connection = setUpMockConnection(202, bos, bis);

    Collection<DoublePointData> doublePointDataCollection =
        new ArrayList<DoublePointData>() {
          {
            add(
                DoublePointData.create(
                    1619687639000000000L, 1619687659000000000L, Attributes.empty(), 123.456d));
            add(
                DoublePointData.create(
                    1619687639000000000L, 1619687659000000000L, Attributes.empty(), 321.456d));
            add(DoublePointData.create(0L, 0L, Attributes.empty(), 654.321d));
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

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();

    CompletableResultCode result =
        metricExporter.export(Collections.singleton(metricData), connection);

    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    assertEquals(
        "doubleGaugeData,dt.metrics.source=opentelemetry gauge,123.456 1619687659000\n"
            + "doubleGaugeData,dt.metrics.source=opentelemetry gauge,321.456 1619687659000\n"
            + "doubleGaugeData,dt.metrics.source=opentelemetry gauge,654.321",
        bos.toString());
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }

  @Test
  void createDoubleSummaryLines() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayInputStream bis =
        new ByteArrayInputStream(
            "{\n\"linesOk\": 3,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

    HttpURLConnection connection = setUpMockConnection(202, bos, bis);

    Collection<DoubleSummaryPointData> doubleSummaryPointDataCollection =
        new ArrayList<DoubleSummaryPointData>() {
          {
            add(
                DoubleSummaryPointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Attributes.empty(),
                    7,
                    500.70d,
                    Arrays.asList(
                        ValueAtPercentile.create(0.0, 0.1),
                        ValueAtPercentile.create(100.0, 100.1))));
            add(
                DoubleSummaryPointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Attributes.empty(),
                    3,
                    202.66d,
                    Arrays.asList(
                        ValueAtPercentile.create(0.0, 0.22),
                        ValueAtPercentile.create(100.0, 123.45))));
            add(
                DoubleSummaryPointData.create(
                    0L,
                    0L,
                    Attributes.empty(),
                    10,
                    300.70d,
                    Arrays.asList(
                        ValueAtPercentile.create(0.0, 0.123),
                        ValueAtPercentile.create(100.0, 234.5))));
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

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();

    CompletableResultCode result =
        metricExporter.export(Collections.singleton(metricData), connection);

    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    assertEquals(
        "doubleSummary,dt.metrics.source=opentelemetry gauge,min=0.1,max=100.1,sum=500.7,count=7 1619687659000\n"
            + "doubleSummary,dt.metrics.source=opentelemetry gauge,min=0.22,max=123.45,sum=202.66,count=3 1619687659000\n"
            + "doubleSummary,dt.metrics.source=opentelemetry gauge,min=0.123,max=234.5,sum=300.7,count=10",
        bos.toString());
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }

  @Test
  void createDoubleHistogramLines() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayInputStream bis =
        new ByteArrayInputStream(
            "{\n\"linesOk\": 2,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

    HttpURLConnection connection = setUpMockConnection(202, bos, bis);

    Collection<DoubleHistogramPointData> doubleHistogramPointDataCollection =
        new ArrayList<DoubleHistogramPointData>() {
          {
            add(
                DoubleHistogramPointData.create(
                    1619687639000000000L,
                    1619687659000000000L,
                    Attributes.empty(),
                    10.123d,
                    Arrays.asList(0.1d, 1.2d, 3.4d, 5.6d),
                    Arrays.asList(0L, 2L, 1L, 3L, 0L)));
            add(
                DoubleHistogramPointData.create(
                    0L,
                    0L,
                    Attributes.empty(),
                    23.45d,
                    Arrays.asList(0.2d, 1.2d, 3.4d, 5.9d),
                    Arrays.asList(0L, 2L, 1L, 3L, 5L)));
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

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();

    CompletableResultCode result =
        metricExporter.export(Collections.singleton(metricData), connection);

    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    assertEquals(
        "doubleHistogram,dt.metrics.source=opentelemetry gauge,min=0.1,max=5.6,sum=10.123,count=6 1619687659000\n"
            + "doubleHistogram,dt.metrics.source=opentelemetry gauge,min=0.2,max=5.9,sum=23.45,count=11",
        bos.toString());
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }

  @Test
  public void testCountertWithViewCumulativeTemporality() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    URL url = mock(URL.class);
    HttpURLConnection connection = mock(HttpURLConnection.class);
    when(url.openConnection()).thenReturn(connection);
    when(connection.getURL()).thenReturn(url);
    when(connection.getOutputStream()).thenReturn(bos);
    when(connection.getResponseCode()).thenReturn(202);
    when(connection.getInputStream())
        .thenReturn(
            new ByteArrayInputStream(
                "{\n\"linesOk\": 1,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes()));

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();

    InMemoryDtMetricReader reader = new InMemoryDtMetricReader(metricExporter);
    SdkMeterProvider sdkMeterProvider =
        SdkMeterProvider.builder()
            .setClock(testClock)
            .registerMetricReader(reader)
            .registerView(
                InstrumentSelector.builder()
                    .setInstrumentType(InstrumentType.COUNTER)
                    .setInstrumentName("testSum")
                    .build(),
                View.builder()
                    .setAggregation(Aggregation.sum(AggregationTemporality.CUMULATIVE))
                    .build())
            .build();

    Meter sdkMeter = sdkMeterProvider.get(getClass().getName());
    LongCounter counter = sdkMeter.counterBuilder("testSum").build();

    counter.add(100);
    sdkMeterProvider.forceFlush();
    testClock.advance(Duration.ofSeconds(1));

    counter.add(200);
    sdkMeterProvider.forceFlush();
    testClock.advance(Duration.ofSeconds(1));

    counter.add(300);
    testClock.advance(Duration.ofSeconds(1));
    CompletableResultCode result = sdkMeterProvider.forceFlush();

    assertTrue(result.isSuccess());
    assertEquals(
        "testSum,dt.metrics.source=opentelemetry count,delta=200 1557212401000testSum,dt.metrics.source=opentelemetry count,delta=300 1557212403000",
        bos.toString());
  }

  @Test
  public void testCounterWithViewDeltaTemporality() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    URL url = mock(URL.class);
    HttpURLConnection connection = mock(HttpURLConnection.class);
    when(url.openConnection()).thenReturn(connection);
    when(connection.getURL()).thenReturn(url);
    when(connection.getOutputStream()).thenReturn(bos);
    when(connection.getResponseCode()).thenReturn(202);
    when(connection.getInputStream())
        .thenReturn(
            new ByteArrayInputStream(
                "{\n\"linesOk\": 1,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes()));

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();

    InMemoryDtMetricReader reader = new InMemoryDtMetricReader(metricExporter);
    SdkMeterProvider sdkMeterProvider =
        SdkMeterProvider.builder()
            .setClock(testClock)
            .registerMetricReader(reader)
            .registerView(
                InstrumentSelector.builder()
                    .setInstrumentType(InstrumentType.COUNTER)
                    .setInstrumentName("testSum")
                    .build(),
                View.builder()
                    .setAggregation(Aggregation.sum(AggregationTemporality.DELTA))
                    .build())
            .build();

    Meter sdkMeter = sdkMeterProvider.get(getClass().getName());
    LongCounter counter = sdkMeter.counterBuilder("testSum").build();

    counter.add(100);
    sdkMeterProvider.forceFlush();
    testClock.advance(Duration.ofSeconds(1));

    counter.add(200);
    testClock.advance(Duration.ofSeconds(1));
    CompletableResultCode result = sdkMeterProvider.forceFlush();

    assertTrue(result.isSuccess());
    assertEquals(
        "testSum,dt.metrics.source=opentelemetry count,delta=100 1557212400000testSum,dt.metrics.source=opentelemetry count,delta=200 1557212402000",
        bos.toString());
  }

  private HttpURLConnection setUpMockConnection(
      int statusCode, ByteArrayOutputStream outputStream, ByteArrayInputStream response)
      throws IOException {
    HttpURLConnection connection = mock(HttpURLConnection.class);
    when(connection.getURL()).thenReturn(new URL("http://localhost"));
    when(connection.getOutputStream()).thenReturn(outputStream);
    when(connection.getResponseCode()).thenReturn(statusCode);

    if (response != null) {
      when(connection.getInputStream())
          .thenReturn(
              new ByteArrayInputStream(
                  "{\n\"linesOk\": 1,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes()));
    }
    return connection;
  }
}
