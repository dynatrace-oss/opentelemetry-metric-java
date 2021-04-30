# Dynatrace OpenTelemetry Metrics Exporter for Java

> This project is developed and maintained by Dynatrace R&D.
Currently, this is a prototype and not intended for production use.
It is not covered by Dynatrace support.

This exporter plugs into the OpenTelemetry Metrics SDK for Java, which is in alpha/preview state and neither considered stable nor complete as of this writing.

See [open-telemetry/opentelemetry-java](https://github.com/open-telemetry/opentelemetry-java) for the current state of the OpenTelemetry SDK for Java.

## Getting started

The general setup of OpenTelemetry Java is explained in the official [Getting Started Guide](https://github.com/open-telemetry/opentelemetry-java/blob/master/QUICKSTART.md).

Using the Metrics API is explained in the [Metrics section](https://github.com/open-telemetry/opentelemetry-java/blob/master/QUICKSTART.md#metrics).

The Dynatrace exporter is added and set up like this:

First, create a `DynatraceMetricExporter`:

```java
DynatraceMetricExporter exporter =
    DynatraceMetricExporter.builder()
      .setApiToken({API_TOKEN})
      .setUrl({INGEST_URL})
      .build();
```

If the exporter is running on a host with an installed OneAgent, it can be initialized with:

```java
DynatraceMetricExporter exporter = DynatraceMetricExporter.getDefault();
```

Then, register the exporter using the `IntervalMetricReader` of OpenTelemetry:

```java
IntervalMetricReader intervalMetricReader =
    IntervalMetricReader.builder()
        .setMetricProducers(
            Collections.singleton(
                OpenTelemetrySdk.getGlobalMeterProvider().getMetricProducer()))
        .setExportIntervalMillis(5000)
        .setMetricExporter(exporter)
        .build();
```

Once metrics are reported using the Metrics API, data will be exported to Dynatrace:

```java
Meter meter = OpenTelemetry.getGlobalMeter("instrumentation-library-name","semver:1.0.0");

LongCounter counter = meter
        .longCounterBuilder("processed_jobs")
        .setDescription("Processed jobs")
        .setUnit("1")
        .build();

counter.add(123, Labels.of("job-type", "print-receipt"));
```

A full setup is provided in our [example project](example/src/main/java/com/dynatrace/opentelemetry/metric/example/DynatraceExporterExample.java)

### Configuration

The exporter allows for configuring the following settings using its builder (`DynatraceMetricExporter.builder()`):

#### Dynatrace API Endpoint

The endpoint to which the metrics are sent is specified using `setUrl`.

Given an environment ID `myenv123` on Dynatrace SaaS, the [metrics ingest endpoint](https://www.dynatrace.com/support/help/dynatrace-api/environment-api/metric-v2/post-ingest-metrics/) would be `https://myenv123.live.dynatrace.com/api/v2/metrics/ingest`.

If a OneAgent is installed on the host, it can provide a local endpoint for providing metrics directly without the need for an API token.
This feature is currently in an Early Adopter phase and has to be enabled as described in the [OneAgent metric API documentation](https://www.dynatrace.com/support/help/how-to-use-dynatrace/metrics/metric-ingestion/ingestion-methods/local-api/).
Using the local API endpoint, the host ID and host name context are automatically added to each metric as dimensions.
The default metric API endpoint exposed by the OneAgent is `http://localhost:14499/metrics/ingest`.

#### Dynatrace API Token

The Dynatrace API token to be used by the exporter is specified using `setApiToken` and could, for example, be read from an environment variable.
It should not be hardcoded into the code, especially if that code is stored in a VCS.

Creating an API token for your Dynatrace environment is described in the [Dynatrace API documentation](https://www.dynatrace.com/support/help/dynatrace-api/basics/dynatrace-api-authentication/).
The scope required for sending metrics is the `Ingest metrics` scope in the **API v2** section:

![API token creation](docs/img/api_token.png)

#### Metric Key Prefix

The `setPrefix` method of the builder specifies an optional prefix, which is prepended to each metric key, separated by a dot (e.g. `<prefix>.<namespace>.<name>`).

#### Default Dimensions

The `setDefaultDimensions` method can be used to optionally specify a list of key/value pairs (a `DimensionList`), which will be added as additional dimensions to all data points.
Dimension keys are unique and will be de-duplicated, and only one dimension value per key will be sent to the server.
Dimensions set on instruments will overwrite default dimensions.

#### Export OneAgent Metadata

The `setEnrichWithOneAgentMetaData` method on the builder can be used to enable OneAgent metadata export.
If running on a host with a OneAgent, setting this option will export metadata collected by the OneAgent to the Dynatrace endpoint.
If no Dynatrace API endpoint is set, the default exporter endpoint will be the OneAgent endpoint, and this option will be set automatically.
Therefore, if no endpoint is specified, a OneAgent is assumed to be running and exported to, including metadata.
If the OneAgent is running and metrics are exported to an explicitly specified endpoint but this method is not called, no OneAgent metadata will be exported.

### Logging

The log-level for the example project is set to print everything that is logged in the program to the console.
This also prints which messages are sent to the server.
If this is too verbose, set the log level (`.level`) in the [logging.properties](example/src/main/resources/logging.properties) to a higher level, e.g. `INFO` or `WARNING`
