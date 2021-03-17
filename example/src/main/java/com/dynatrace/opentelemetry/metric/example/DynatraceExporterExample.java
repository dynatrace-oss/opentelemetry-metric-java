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
package com.dynatrace.opentelemetry.metric.example;

import com.dynatrace.opentelemetry.metric.DynatraceMetricExporter;
import io.opentelemetry.api.metrics.BoundLongCounter;
import io.opentelemetry.api.metrics.GlobalMetricsProvider;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.common.Labels;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.IntervalMetricReader;
import java.util.Collections;
import java.util.Random;

public class DynatraceExporterExample {

  static {
    // read logging.properties to set up the logging levels.
    String path =
        DynatraceExporterExample.class.getClassLoader().getResource("logging.properties").getFile();

    System.setProperty("java.util.logging.config.file", path);
  }

  private static final Random random = new Random();

  public static void main(String[] args) throws Exception {
    DynatraceMetricExporter exporter;
    if (args.length == 2) {
      // Endpoint URL and API token passed as args
      String endpointUrl = args[0];
      String apiToken = args[1];
      System.out.println("Setting up DynatraceMetricExporter to export to " + endpointUrl);
      Labels defaultDimensions = Labels.of("environment", "staging");
      exporter =
          DynatraceMetricExporter.builder()
              .setUrl(endpointUrl)
              .setApiToken(apiToken)
              .setPrefix("otel.java")
              .setDefaultDimensions(defaultDimensions)
              .build();
    } else {
      // default is to export to local OneAgent
      System.out.println("No endpoint URL and API token passed as command line args");
      System.out.println("Setting up DynatraceMetricExporter to export to local OneAgent endpoint");
      exporter = DynatraceMetricExporter.getDefault();
    }

    SdkMeterProvider provider = SdkMeterProvider.builder().buildAndRegisterGlobal();
    IntervalMetricReader intervalMetricReader =
        IntervalMetricReader.builder()
            .setMetricProducers(Collections.singleton(provider))
            .setExportIntervalMillis(5000)
            .setMetricExporter(exporter)
            .build();

    // Gets or creates a named meter instance
    Meter meter =
        GlobalMetricsProvider.getMeter(DynatraceExporterExample.class.getName(), "0.1.0-beta");

    // Create a counter
    LongCounter counter =
        meter
            .longCounterBuilder("example_counter")
            .setDescription("Just some counter used as an example")
            .setUnit("1")
            .build();

    // Use a bound counter with a pre-defined label set
    BoundLongCounter someWorkCounter =
        counter.bind(Labels.of("bound_dimension", "dimension_value"));

    while (true) {
      // Record data with bound labels
      someWorkCounter.add(random.nextInt(5));

      // Or record data on unbound counter and explicitly specify the label set at call-time
      counter.add(random.nextInt(10), Labels.of("environment", "testing"));
      counter.add(random.nextInt(20), Labels.of("environment", "staging"));

      Thread.sleep(1000);
    }
  }
}
