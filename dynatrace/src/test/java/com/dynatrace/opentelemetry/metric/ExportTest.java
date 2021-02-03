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
import io.opentelemetry.api.common.Labels;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.common.InstrumentationLibraryInfo;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.DoubleSumData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class ExportTest {

  public static MetricData generateMetricData() {
    return MetricData.createDoubleSum(
        Resource.create(Attributes.builder().build()),
        InstrumentationLibraryInfo.getEmpty(),
        "name",
        "desc",
        "",
        DoubleSumData.create(
            true,
            AggregationTemporality.CUMULATIVE,
            Collections.singleton(DoublePointData.create(123, 4560000, Labels.empty(), 194.0))));
  }

  @Test
  public void testExport() throws IOException {
    MetricData md = generateMetricData();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    HttpURLConnection connection = mock(HttpURLConnection.class);
    when(connection.getURL()).thenReturn(new URL("http://localhost"));
    when(connection.getOutputStream()).thenReturn(bos);
    when(connection.getResponseCode()).thenReturn(202);

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();

    CompletableResultCode result = metricExporter.export(Collections.singleton(md), connection);

    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
    assertEquals(bos.toString(), MetricAdapter.toMint(Collections.singleton(md)).serialize());
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
}
