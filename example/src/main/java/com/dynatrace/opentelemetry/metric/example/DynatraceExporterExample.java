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
import io.opentelemetry.api.metrics.GlobalMeterProvider;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.common.Labels;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.IntervalMetricReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Objects;
import java.util.Random;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class DynatraceExporterExample {
  private static final Logger logger = Logger.getLogger(DynatraceExporterExample.class.getName());
  private static final Random random = new Random();

  static {
    // read logging.properties and set it to the global LogManager.
    LogManager logManager = LogManager.getLogManager();
    try {
      logManager.readConfiguration(
          new FileInputStream(
              Objects.requireNonNull(
                      DynatraceExporterExample.class
                          .getClassLoader()
                          .getResource("logging.properties"))
                  .getFile()));
    } catch (NullPointerException | IOException e) {
      logger.warning("Failed to read logging setup from logging.properties: " + e.getMessage());
    }
  }

  public static void main(String[] args) throws Exception {
    // Create a DynatraceMetricExporter. This method tries to create one from environment variables,
    // then from program arguments, and falls back to the default OneAgent endpoint if nothing is
    // set.
    DynatraceMetricExporter exporter = getExampleExporter(args);

    // Create a metrics producer and a meter provider (SdkMeterProvider is both). As noted in the
    // documentation (https://opentelemetry.io/docs/java/manual_instrumentation/, under the metrics
    // section), the APIs for acquiring a MeterProvider are in flux and the example below is
    // probably outdated. Please consult the OpenTelemetry documentation for more information on
    // the preferred way to acquire a MeterProvider.
    SdkMeterProvider provider = SdkMeterProvider.builder().buildAndRegisterGlobal();

    // Set the Dynatrace exporter to read from the provider created above (in this case the global
    // meter provider).
    IntervalMetricReader.builder()
        .setMetricProducers(Collections.singleton(provider))
        .setExportIntervalMillis(60000)
        .setMetricExporter(exporter)
        .build()
        .start();

    // Get or create a named meter instance. If a reference to the Provider ist kept,
    // provider.get(...) would do the same.
    Meter meter =
        GlobalMeterProvider.getMeter(DynatraceExporterExample.class.getName(), "0.1.0-beta");

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

  private static DynatraceMetricExporter getExampleExporter(String[] args) {
    DynatraceMetricExporter exporter;

    logger.info("Trying to create a DynatraceMetricExporter from environment variables.");
    exporter = tryGetExporterFromEnvironmentVariables();

    if (exporter == null) {
      logger.info(
          "Trying to create a DynatraceMetricExporter from the first two program arguments.");
      exporter = tryGetExporterFromArgs(args);
    }

    if (exporter == null) {
      logger.info("Falling back to the default OneAgent exporter.");
      exporter = DynatraceMetricExporter.getDefault();
    }

    return exporter;
  }

  private static DynatraceMetricExporter tryGetExporterFromArgs(String[] args) {
    if (args.length >= 2) {
      return makeExampleExporter(args[0], args[1]);
    }
    logger.info("Failed to set up a DynatraceMetricExporter from program arguments.");
    return null;
  }

  private static DynatraceMetricExporter tryGetExporterFromEnvironmentVariables() {
    String endpoint = System.getenv("DYNATRACEAPI_METRICS_INGEST_ENDPOINT");
    String token = System.getenv("DYNATRACEAPI_METRICS_INGEST_TOKEN");

    if (endpoint != null && !endpoint.isEmpty()) {
      logger.info(String.format("Endpoint read from environment: %s", endpoint));
      if (token == null) {
        logger.info(
            "No token set in environment. Assuming that the endpoint is a local OneAgent that does not require an API token.");
      } else {
        logger.info("Token read from the environment.");
      }
      // this will pass null to the exporter when no token is set.
      return makeExampleExporter(endpoint, token);
    }

    logger.info("Failed to set up DynatraceMetricExporter from environment variables.");
    return null;
  }

  private static DynatraceMetricExporter makeExampleExporter(String endpoint, String token) {
    try {
      Labels exampleDimensions = Labels.of("environment", "example");
      return DynatraceMetricExporter.builder()
          .setPrefix("otel.java")
          .setDefaultDimensions(exampleDimensions)
          .setUrl(endpoint)
          .setApiToken(token)
          .build();
    } catch (MalformedURLException e) {
      logger.warning(String.format("Endpoint '%s' is not a valid URL.", endpoint));
      return null;
    }
  }
}
