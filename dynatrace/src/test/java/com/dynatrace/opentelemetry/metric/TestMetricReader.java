package com.dynatrace.opentelemetry.metric;

import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.MetricProducer;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.MetricReaderFactory;
import java.util.Collection;
import java.util.Collections;

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
  public MetricReader apply(MetricProducer producer) {
    this.metricProducer = producer;
    return this;
  }
}
