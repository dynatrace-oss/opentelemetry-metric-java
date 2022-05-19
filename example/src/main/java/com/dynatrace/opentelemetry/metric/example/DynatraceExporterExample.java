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

import static io.opentelemetry.api.common.AttributeKey.stringKey;

import com.dynatrace.opentelemetry.metric.DynatraceMetricExporter;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleUpDownCounter;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.time.Duration;
import java.util.Objects;
import java.util.Random;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class DynatraceExporterExample {
  private static final Logger logger = Logger.getLogger(DynatraceExporterExample.class.getName());
  private static final Random random = new Random();

  public static void main(String[] args) throws Exception {
    // Create a DynatraceMetricExporter. This method tries to create one from environment variables,
    // then from program arguments, and falls back to the default OneAgent endpoint if nothing is
    // set.
    DynatraceMetricExporter exporter = getExampleExporter(args);

    // Creates the meter provider, configuring the metric reader and setting the Dynatrace exporter.
    SdkMeterProvider meterProvider =
        SdkMeterProvider.builder()
            .registerMetricReader(
                PeriodicMetricReader.builder(exporter).setInterval(Duration.ofSeconds(30)).build())
            .build();
    // set up this meter provider as the global meter provider. The global context allows access
    // from anywhere in the program.
    OpenTelemetrySdk.builder().setMeterProvider(meterProvider).buildAndRegisterGlobal();

    // Get or create a named meter instance. If a reference to the MeterProvider ist kept,
    // meterProvider.meterBuilder(...) would do the same.
    Meter meter =
        GlobalOpenTelemetry.getMeterProvider()
            .meterBuilder(DynatraceExporterExample.class.getName())
            .setInstrumentationVersion("0.5.0-alpha")
            .build();

    // Create a counter
    LongCounter counter =
        meter
            .counterBuilder("example_counter")
            .setDescription("Just some counter used as an example")
            .setUnit("1")
            .build();

    // Create an UpDownCounter
    DoubleUpDownCounter upDownCounter =
        meter.upDownCounterBuilder("updown_counter").ofDoubles().build();

    // the gauge callback is called once on every export.
    meter
        .gaugeBuilder("example_gauge")
        .setDescription("a random percentage")
        .setUnit("percent")
        .buildWithCallback(gauge -> gauge.record(random.nextDouble() * 100));

    AttributeKey<String> attributeKeyEnvironment = stringKey("environment");

    int sign = 1;

    for (int i = 0; i < Integer.MAX_VALUE; i++) {
      // Record some data with attributes, then sleep for some time.
      counter.add(random.nextInt(10), Attributes.of(attributeKeyEnvironment, "testing"));
      counter.add(random.nextInt(20), Attributes.of(attributeKeyEnvironment, "staging"));

      // updowncounter grows and gets smaller over time, dependent on sign.
      upDownCounter.add(random.nextDouble() * sign);

      if (random.nextInt(90) <= 1) {
        // flip the sign randomly and seldomly (roughly every 90 seconds).
        sign = sign * -1;
      }

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
      logger.info(() -> String.format("Endpoint read from environment: %s", endpoint));
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
      Attributes exampleDimensions = Attributes.of(stringKey("environment"), "example");
      return DynatraceMetricExporter.builder()
          .setPrefix("otel.java")
          .setDefaultDimensions(exampleDimensions)
          .setUrl(endpoint)
          .setApiToken(token)
          .setEnrichWithOneAgentMetaData(true)
          .build();
    } catch (MalformedURLException e) {
      logger.warning(String.format("Endpoint '%s' is not a valid URL.", endpoint));
      return null;
    }
  }
}
