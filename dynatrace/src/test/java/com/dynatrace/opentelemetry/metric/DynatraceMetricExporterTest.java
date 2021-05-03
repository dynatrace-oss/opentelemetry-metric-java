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
import static org.mockito.Mockito.*;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.common.Labels;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.DoubleSumData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class DynatraceMetricExporterTest {

  public static MetricData generateMetricData() {
    return generateMetricDataWithLabels(Labels.empty());
  }

  public static MetricData generateMetricDataWithLabels(Labels labels) {
    return MetricData.createDoubleSum(
        Resource.create(Attributes.builder().build()),
        InstrumentationLibraryInfo.empty(),
        "name",
        "desc",
        "",
        DoubleSumData.create(
            true,
            AggregationTemporality.CUMULATIVE,
            Collections.singleton(
                DoublePointData.create(
                    1619687639000000000L, 1619687659000000000L, labels, 194.0))));
  }

  @Test
  public void testExport() throws IOException {
    MetricData md = generateMetricData();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    HttpURLConnection connection = mock(HttpURLConnection.class);
    when(connection.getURL()).thenReturn(new URL("http://localhost"));
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

    CompletableResultCode result = metricExporter.export(Collections.singleton(md), connection);

    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    assertEquals("name,dt.metrics.source=opentelemetry count,194.0 1619687659000", bos.toString());
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }

  @Test
  public void testFailedExport() throws IOException {
    MetricData md = generateMetricData();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    HttpURLConnection connection = mock(HttpURLConnection.class);
    when(connection.getURL()).thenReturn(new URL("http://localhost"));
    when(connection.getOutputStream()).thenReturn(bos);
    when(connection.getResponseCode()).thenReturn(400);

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
    HttpURLConnection connection = mock(HttpURLConnection.class);
    when(connection.getURL()).thenReturn(new URL("http://localhost"));
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
            .setPrefix("prefix")
            .build();

    CompletableResultCode result = metricExporter.export(Collections.singleton(md), connection);

    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    assertEquals(
        "prefix.name,dt.metrics.source=opentelemetry count,194.0 1619687659000", bos.toString());
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }

  @Test
  public void addDefaultDimensions() throws IOException {
    MetricData md = generateMetricData();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    HttpURLConnection connection = mock(HttpURLConnection.class);
    when(connection.getURL()).thenReturn(new URL("http://localhost"));
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
            .setDefaultDimensions(Labels.of("default", "value"))
            .build();

    CompletableResultCode result = metricExporter.export(Collections.singleton(md), connection);

    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    assertEquals(
        "name,default=value,dt.metrics.source=opentelemetry count,194.0 1619687659000",
        bos.toString());
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }

  @Test
  public void testWithLabels() throws IOException {
    MetricData md = generateMetricDataWithLabels(Labels.of("label1", "val1", "label2", "val2"));
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    HttpURLConnection connection = mock(HttpURLConnection.class);
    when(connection.getURL()).thenReturn(new URL("http://localhost"));
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

    CompletableResultCode result = metricExporter.export(Collections.singleton(md), connection);

    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    assertEquals(
        "name,dt.metrics.source=opentelemetry,label1=val1,label2=val2 count,194.0 1619687659000",
        bos.toString());
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }
}
