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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import com.dynatrace.metric.util.Dimension;
import com.dynatrace.metric.util.DimensionList;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.util.Collections;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class DynatraceMetricExporterTest {

  @Test
  void testExport() throws IOException {
    MetricData md = generateValidDoubleSumData();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayInputStream bis =
        new ByteArrayInputStream(
            "{\n\"linesOk\": 1,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

    HttpURLConnection connection = setUpMockConnection(202, bos, bis);

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setEnrichWithOneAgentMetaData(false)
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();

    CompletableResultCode result = metricExporter.doExport(Collections.singleton(md), connection);

    assertExportRequestSuccess(
        connection,
        String.format(
            "%s,dt.metrics.source=opentelemetry count,delta=194.0 %d", DEFAULT_NAME, MILLIS_TS_2),
        bos.toString(),
        result);
  }

  @Test
  void testExportWrongResponseCode() throws IOException {
    MetricData md = generateValidDoubleSumData();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayInputStream bis =
        new ByteArrayInputStream(
            "{\n\"linesOk\": 1,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

    // 200 is not a failure, but not the response we expect, as we expect 202.
    HttpURLConnection connection = setUpMockConnection(200, bos, bis);

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setEnrichWithOneAgentMetaData(false)
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();

    assertThat(metricExporter.doExport(Collections.singleton(md), connection))
        .isEqualTo(CompletableResultCode.ofFailure());
  }

  @Test
  void testFailedExport() throws IOException {
    MetricData md = generateValidDoubleSumData();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();

    HttpURLConnection connection = setUpMockConnection(400, bos, null);

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setEnrichWithOneAgentMetaData(false)
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();

    CompletableResultCode result = metricExporter.doExport(Collections.singleton(md), connection);

    assertEquals(CompletableResultCode.ofFailure(), result);
  }

  @Test
  void testAddPrefix() throws IOException {
    MetricData md = generateValidDoubleSumData();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayInputStream bis =
        new ByteArrayInputStream(
            "{\n\"linesOk\": 1,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

    HttpURLConnection connection = setUpMockConnection(202, bos, bis);

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setEnrichWithOneAgentMetaData(false)
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .setPrefix("prefix")
            .build();

    CompletableResultCode result = metricExporter.doExport(Collections.singleton(md), connection);

    assertExportRequestSuccess(
        connection,
        String.format(
            "prefix.%s,dt.metrics.source=opentelemetry count,delta=194.0 %d",
            DEFAULT_NAME, MILLIS_TS_2),
        bos.toString(),
        result);
  }

  @Test
  void addDefaultDimensions() throws IOException {
    MetricData md = generateValidDoubleSumData();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayInputStream bis =
        new ByteArrayInputStream(
            "{\n\"linesOk\": 1,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

    HttpURLConnection connection = setUpMockConnection(202, bos, bis);

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setEnrichWithOneAgentMetaData(false)
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .setDefaultDimensions(Attributes.of(AttributeKey.stringKey("default"), "value"))
            .build();

    CompletableResultCode result = metricExporter.doExport(Collections.singleton(md), connection);

    assertExportRequestSuccess(
        connection,
        String.format(
            "%s,default=value,dt.metrics.source=opentelemetry count,delta=194.0 %d",
            DEFAULT_NAME, MILLIS_TS_2),
        bos.toString(),
        result);
  }

  @Test
  void testWithAttributes() throws IOException {
    Attributes attributes = Attributes.builder().put("attr1", "val1").put("attr2", "val2").build();
    MetricData md = generateValidDoubleSumData(attributes);

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteArrayInputStream bis =
        new ByteArrayInputStream(
            "{\n\"linesOk\": 1,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

    HttpURLConnection connection = setUpMockConnection(202, bos, bis);

    DynatraceMetricExporter metricExporter =
        DynatraceMetricExporter.builder()
            .setEnrichWithOneAgentMetaData(false)
            .setApiToken("mytoken")
            .setUrl(connection.getURL())
            .build();

    CompletableResultCode result = metricExporter.doExport(Collections.singleton(md), connection);

    assertRequestProperties(connection);

    String actual = bos.toString();

    // Even though Attributes are sorted internally, they might be serialized in an unsorted manner.
    // This is because the utils library uses a HashMap to merge all dimensions.
    assertThat(actual)
        .contains("dt.metrics.source=opentelemetry")
        .contains("attr1=val1")
        .contains("attr2=val2")
        .startsWith(DEFAULT_NAME)
        .endsWith(String.format("count,delta=194.0 %d", MILLIS_TS_2));
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }

  @Test
  void testDynatraceMetadata() throws IOException {
    try (MockedStatic<DimensionList> dimensionListMock = mockStatic(DimensionList.class)) {
      // call the real methods except for the fromDynatraceMetadata method.
      dimensionListMock
          .when(() -> DimensionList.create(Mockito.any(Dimension.class)))
          .thenCallRealMethod();
      dimensionListMock
          .when(() -> DimensionList.fromCollection(Mockito.any()))
          .thenCallRealMethod();
      dimensionListMock
          .when(() -> DimensionList.merge(Mockito.any(DimensionList.class)))
          .thenCallRealMethod();
      DimensionList metadata =
          DimensionList.create(
              Dimension.create("dt.metadata.one", "value_one"),
              Dimension.create("dt.metadata.two", "value_two"));
      dimensionListMock.when(DimensionList::fromDynatraceMetadata).thenReturn(metadata);

      MetricData md = generateValidDoubleSumData();

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ByteArrayInputStream bis =
          new ByteArrayInputStream(
              "{\n\"linesOk\": 1,\n\"linesInvalid\": 0,\n  \"error\": null\n}".getBytes());

      HttpURLConnection connection = setUpMockConnection(202, bos, bis);

      DynatraceMetricExporter metricExporter =
          DynatraceMetricExporter.builder()
              .setEnrichWithOneAgentMetaData(true) // trigger retrieval of DynatraceMetadata
              .setApiToken("mytoken")
              .setUrl(connection.getURL())
              .build();

      CompletableResultCode result = metricExporter.doExport(Collections.singleton(md), connection);
      assertThat(result).isEqualTo(CompletableResultCode.ofSuccess());
      assertThat(bos.toString())
          .contains("dt.metadata.one=value_one")
          .contains("dt.metadata.two=value_two")
          .contains("dt.metrics.source=opentelemetry")
          .startsWith(DEFAULT_NAME)
          .endsWith(String.format("count,delta=194.0 %d", MILLIS_TS_2));

      assertRequestProperties(connection);
    }
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

  @Test
  void testSerializeToMetricLines_CallsCorrectSerializerMethod_LongSum() {
    Serializer serializerMock = mock(Serializer.class);
    DynatraceMetricExporter exporter =
        new DynatraceMetricExporter(mock(URL.class), "", serializerMock);
    MetricData metricData = mock(MetricData.class);

    when(metricData.getType()).thenReturn(MetricDataType.LONG_SUM);
    exporter.serializeToMetricLines(Collections.singletonList(metricData));
    verify(serializerMock).createLongSumLines(metricData);
  }

  @Test
  void testSerializeToMetricLines_CallsCorrectSerializerMethod_LongGauge() {
    Serializer serializerMock = mock(Serializer.class);
    DynatraceMetricExporter exporter =
        new DynatraceMetricExporter(mock(URL.class), "", serializerMock);
    MetricData metricData = mock(MetricData.class);

    when(metricData.getType()).thenReturn(MetricDataType.LONG_GAUGE);
    exporter.serializeToMetricLines(Collections.singletonList(metricData));
    verify(serializerMock).createLongGaugeLines(metricData);
  }

  @Test
  void testSerializeToMetricLines_CallsCorrectSerializerMethod_DoubleSum() {
    Serializer serializerMock = mock(Serializer.class);
    DynatraceMetricExporter exporter =
        new DynatraceMetricExporter(mock(URL.class), "", serializerMock);
    MetricData metricData = mock(MetricData.class);

    when(metricData.getType()).thenReturn(MetricDataType.DOUBLE_SUM);
    exporter.serializeToMetricLines(Collections.singletonList(metricData));
    verify(serializerMock).createDoubleSumLines(metricData);
  }

  @Test
  void testSerializeToMetricLines_CallsCorrectSerializerMethod_DoubleGauge() {
    Serializer serializerMock = mock(Serializer.class);
    DynatraceMetricExporter exporter =
        new DynatraceMetricExporter(mock(URL.class), "", serializerMock);
    MetricData metricData = mock(MetricData.class);

    when(metricData.getType()).thenReturn(MetricDataType.DOUBLE_GAUGE);
    exporter.serializeToMetricLines(Collections.singletonList(metricData));
    verify(serializerMock).createDoubleGaugeLines(metricData);
  }

  @Test
  void testSerializeToMetricLines_CallsCorrectSerializerMethod_Summary() {
    Serializer serializerMock = mock(Serializer.class);
    DynatraceMetricExporter exporter =
        new DynatraceMetricExporter(mock(URL.class), "", serializerMock);
    MetricData metricData = mock(MetricData.class);

    when(metricData.getType()).thenReturn(MetricDataType.SUMMARY);
    exporter.serializeToMetricLines(Collections.singletonList(metricData));
    verify(serializerMock).createDoubleSummaryLines(metricData);
  }

  @Test
  void testSerializeToMetricLines_CallsCorrectSerializerMethod_Histogram() {
    Serializer serializerMock = mock(Serializer.class);
    DynatraceMetricExporter exporter =
        new DynatraceMetricExporter(mock(URL.class), "", serializerMock);
    MetricData metricData = mock(MetricData.class);

    when(metricData.getType()).thenReturn(MetricDataType.HISTOGRAM);
    exporter.serializeToMetricLines(Collections.singletonList(metricData));
    verify(serializerMock).createDoubleHistogramLines(metricData);
  }

  @Test
  void testSerializeToMetricLines_CallsCorrectSerializerMethod_ExponentialHistogram() {
    Serializer serializerMock = mock(Serializer.class);
    DynatraceMetricExporter exporter =
        new DynatraceMetricExporter(mock(URL.class), "", serializerMock);
    MetricData metricData = mock(MetricData.class);

    when(metricData.getType()).thenReturn(MetricDataType.EXPONENTIAL_HISTOGRAM);
    exporter.serializeToMetricLines(Collections.singletonList(metricData));
    // no interactions should have occurred with the mock, as we don't deal with Exponential
    // Histogram yet.
    verifyNoInteractions(serializerMock);
  }

  @ParameterizedTest
  @MethodSource("provideInstrumentTypes")
  void testGetRightAggregationTemporalityForType(
      InstrumentType type, AggregationTemporality temporality) {
    DynatraceMetricExporter exporter =
        DynatraceMetricExporter.builder().setEnrichWithOneAgentMetaData(false).build();
    assertThat(exporter.getAggregationTemporality(type)).isEqualTo(temporality);
  }

  @Test
  void testPublicExportInvalidUrl() throws IOException {
    URL urlMock = mock(URL.class);
    when(urlMock.openConnection()).thenThrow(new IOException("mocked exception"));

    // the serializer is not used, as the exception is thrown much earlier.
    DynatraceMetricExporter exporter =
        new DynatraceMetricExporter(urlMock, "test", mock(Serializer.class));
    assertThat(exporter.export(Mockito.any())).isEqualTo(CompletableResultCode.ofFailure());
  }

  @Test
  void testPublicExportValidUrl() throws IOException {
    URL urlMock = mock(URL.class);
    when(urlMock.openConnection()).thenReturn(mock(HttpURLConnection.class));

    DynatraceMetricExporter exporter =
        new DynatraceMetricExporter(urlMock, "test", mock(Serializer.class));

    // empty list export should return a CompletableResultCode.ofSuccess from doExport
    assertThat(exporter.export(Collections.emptyList()))
        .isEqualTo(CompletableResultCode.ofSuccess());
  }

  @Test
  void testConnectionThrowsException() throws IOException {
    URL urlMock = mock(URL.class);
    HttpURLConnection connectionMock = mock(HttpURLConnection.class);
    Serializer serializerMock = mock(Serializer.class);
    MetricData metricDataMock = mock(MetricData.class);

    when(urlMock.openConnection()).thenReturn(mock(HttpURLConnection.class));
    doThrow(new ProtocolException("mocked ex"))
        .when(connectionMock)
        .setRequestMethod(Mockito.anyString());
    when(metricDataMock.getType()).thenReturn(MetricDataType.LONG_GAUGE);
    when(serializerMock.createLongGaugeLines(Mockito.any(MetricData.class)))
        .thenReturn(Collections.singletonList("a gauge,3"));

    DynatraceMetricExporter exporter = new DynatraceMetricExporter(urlMock, "test", serializerMock);
    assertThat(exporter.doExport(Collections.singletonList(metricDataMock), connectionMock))
        .isEqualTo(CompletableResultCode.ofFailure());
  }

  @Test
  void testFlush() {
    DynatraceMetricExporter exporter =
        new DynatraceMetricExporter(mock(URL.class), "test", mock(Serializer.class));
    assertThat(exporter.flush()).isEqualTo(CompletableResultCode.ofSuccess());
  }

  @Test
  void testShutdown() {
    DynatraceMetricExporter exporter =
        new DynatraceMetricExporter(mock(URL.class), "test", mock(Serializer.class));
    assertThat(exporter.shutdown()).isEqualTo(CompletableResultCode.ofSuccess());
  }

  private static Stream<Arguments> provideInstrumentTypes() {
    return Stream.of(
        Arguments.of(InstrumentType.COUNTER, AggregationTemporality.DELTA),
        Arguments.of(InstrumentType.OBSERVABLE_COUNTER, AggregationTemporality.DELTA),
        Arguments.of(InstrumentType.HISTOGRAM, AggregationTemporality.DELTA),
        Arguments.of(InstrumentType.UP_DOWN_COUNTER, AggregationTemporality.CUMULATIVE),
        Arguments.of(InstrumentType.OBSERVABLE_UP_DOWN_COUNTER, AggregationTemporality.CUMULATIVE));
  }

  private static MetricData generateValidDoubleSumData() {
    return generateValidDoubleSumData(EMPTY_ATTRIBUTES);
  }

  private static MetricData generateValidDoubleSumData(Attributes attributes) {
    return ImmutableMetricData.createDoubleSum(
        DEFAULT_RESOURCE,
        DEFAULT_SCOPE,
        DEFAULT_NAME,
        DEFAULT_DESC,
        DEFAULT_UNIT,
        ImmutableSumData.create(
            true,
            AggregationTemporality.DELTA,
            Collections.singleton(
                ImmutableDoublePointData.create(NANOS_TS_1, NANOS_TS_2, attributes, 194.0))));
  }

  private void assertExportRequestSuccess(
      HttpURLConnection connection,
      String expectedMetricPayload,
      String actualMetricPayload,
      CompletableResultCode result)
      throws IOException {
    assertRequestProperties(connection);

    assertEquals(expectedMetricPayload, actualMetricPayload);
    assertEquals(CompletableResultCode.ofSuccess(), result);
  }

  private void assertRequestProperties(HttpURLConnection connection) throws ProtocolException {
    verify(connection).setRequestMethod("POST");
    verify(connection).setRequestProperty("Authorization", "Api-Token mytoken");
    verify(connection).setRequestProperty("Content-Type", "text/plain; charset=utf-8");
  }
}
