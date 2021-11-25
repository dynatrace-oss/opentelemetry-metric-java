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

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.MetricProducer;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.MetricReaderFactory;
import java.util.Collection;
import java.util.Collections;
import javax.annotation.Nonnull;

/**
 * Inspired by
 * https://github.com/open-telemetry/opentelemetry-java/blob/main/sdk/metrics/src/main/java/io/opentelemetry/sdk/metrics/testing/InMemoryMetricReader.java
 * A reader that calls the exporter whenever it receives a flush call from the MeterProvider. Useful
 * for testing the exporter working with a configured MeterProvider
 */
class TestMetricReader implements MetricReader, MetricReaderFactory {
  private MetricProducer metricProducer;
  private final MetricExporter exporter;
  private volatile Collection<MetricData> latest = Collections.emptyList();

  TestMetricReader(MetricExporter exporter) {
    this.exporter = exporter;
  }

  @Override
  public CompletableResultCode flush() {
    if (metricProducer != null) {
      latest = metricProducer.collectAllMetrics();
      this.exporter.export(latest);
    }
    return CompletableResultCode.ofSuccess();
  }

  @Override
  public CompletableResultCode shutdown() {
    return CompletableResultCode.ofSuccess();
  }

  @Override
  public MetricReader apply(@Nonnull MetricProducer producer) {
    this.metricProducer = producer;
    return this;
  }
}
