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
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dynatrace.opentelemetry.metric.mint.Datapoint;
import com.dynatrace.opentelemetry.metric.mint.Dimension;
import com.dynatrace.opentelemetry.metric.mint.MintMetricsMessage;
import io.opentelemetry.api.common.Labels;
import io.opentelemetry.api.metrics.DoubleValueRecorder;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import java.util.Collection;
import org.junit.jupiter.api.Test;

public class NormalizationTest {
  private static final SdkMeterProvider provider =
      SdkMeterProvider.builder().buildAndRegisterGlobal();
  private static final Meter meter = provider.get("test$lib", "test:1.0.0");

  //  private static final DoubleSumObserver doubleSumObserver =
  //      meter
  //          .doubleSumObserverBuilder("azure.latency")
  //          .setConstantLabels(Labels.of("AüS-s_3", "k8"))
  //          .build();

  private static final DoubleValueRecorder recorder =
      meter
          .doubleValueRecorderBuilder("latency_service01")
          .setDescription("Measures latency for outgoing gRPC calls")
          .setUnit("ms")
          .build();

  private static final LongCounter byteCounter =
      meter.longCounterBuilder("byte_received").setUnit("Byte").build();

  @Test
  public void invalidLineProtocolLineTest() {
    byteCounter.add(4, Labels.empty());
    byteCounter.add(2, Labels.of("..", ".")); // invalid -> dropped
    byteCounter.add(42, Labels.empty());
    Collection<MetricData> metricData = provider.collectAllMetrics();
    MintMetricsMessage msg = MetricAdapter.toMint(metricData);
    assertEquals(1, msg.datapoints().size());
    assertEquals(0, msg.datapoints().get(0).dimensions().size());
    String expected =
        Datapoint.create("byte_received")
            .timestamp(0)
            .value(Values.longCount(46, false))
            .build()
            .serialize();
    String received = msg.datapoints().get(0).serialize();
    assertEquals(
        expected.substring(0, expected.lastIndexOf(" ")),
        received.substring(0, received.lastIndexOf(" ")));
  }

  @Test
  public void emptyDimensionsTest() {
    recorder.record(42.0, Labels.empty());
    Collection<MetricData> metricData = provider.collectAllMetrics();
    MintMetricsMessage msg = MetricAdapter.toMint(metricData);
    assertEquals(1, msg.datapoints().size());
  }

  @Test
  public void metricKeyTest() throws DynatraceExporterException {

    MetricAdapter.toMintMetricKey("a..b");
    MetricAdapter.toMintMetricKey("asd");
    MetricAdapter.toMintMetricKey(".");
    MetricAdapter.toMintMetricKey(".a");
    MetricAdapter.toMintMetricKey("a.");
    assertThrows(
        DynatraceExporterException.class,
        () -> MetricAdapter.toMintMetricKey("8t.a3241.c_1.e-5.t--.z__.Aa1.Bb2.Cc3.Dd4"));
    MetricAdapter.toMintMetricKey("test..e12.test");
    MetricAdapter.toMintMetricKey("f5.distribution");

    assertEquals("test______test.a__", MetricAdapter.toMintMetricKey("test?$%&!=test.a+#"));
    assertEquals(
        "a_b_c_d_e_f_g_h_i_j_k_l_m-n.o_p:q_r_s_t_u_v_w_x_y_z_0_1_2_3_4_5_6_7.acht",
        MetricAdapter.toMintMetricKey(
            "a!b\"c#d$e%f&g'h(i)j*k+l,m-n.o/p:q;r<s=t>u?v@w[x]y\\z^0 1_2`3{4|5}6~7.acht"));
    assertEquals(
        "t8.a3241.c_1.e-5.t--.z__.Aa1.Bb2.Cc3.Dd4",
        MetricAdapter.toMintMetricKey("t8.a3241.c_1.e-5.t--.z__.Aa1.Bb2.Cc3.Dd4"));
  }

  @Test
  public void dimensionTest() throws DynatraceExporterException {
    assertEquals(
        Dimension.create("test_____", "test?$%&!"),
        MetricAdapter.toMintDimension("test?$%&!", "test?$%&!"));
  }

  @Test
  public void dimensionKeyTest() throws DynatraceExporterException {
    assertThrows(DynatraceExporterException.class, () -> MetricAdapter.toMintDimension(".", "b"));
    MetricAdapter.toMintDimensionKey("test..e12");
    assertThrows(DynatraceExporterException.class, () -> MetricAdapter.toMintDimension(".a", "b"));
    assertThrows(DynatraceExporterException.class, () -> MetricAdapter.toMintDimension("a.", "b"));
    MetricAdapter.toMintDimension("a..b", "b");
  }

  @Test
  public void dimensionValueTest() throws DynatraceExporterException {
    assertEquals("test..e12", MetricAdapter.toMintDimensionValue("test..e12"));
    assertEquals("\\\"test..e12\\\"", MetricAdapter.toMintDimensionValue("\"test..e12\""));
    assertEquals("\\\"\\\"\\\"\\\"", MetricAdapter.toMintDimensionValue("\"\"\"\""));
    assertEquals("test e12", MetricAdapter.toMintDimensionValue("test e12"));
    assertEquals("test?$%&!", MetricAdapter.toMintDimensionValue("test?$%&!"));
    assertEquals("\\\"\\\\", MetricAdapter.toMintDimensionValue("\"\\"));
    assertEquals("\\\\\\\\\\\\", MetricAdapter.toMintDimensionValue("\\\\\\"));
  }
}
