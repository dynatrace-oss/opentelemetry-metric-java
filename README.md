# Dynatrace OpenTelemetry Metrics Exporter for Java

> This exporter is based on the OpenTelemetry Metrics SDK for Java, which is currently in an alpha state and neither considered stable nor complete as of this writing.
> As such, this exporter is not intended for production use until the underlying OpenTelemetry Metrics API and SDK are stable.
> See [open-telemetry/opentelemetry-java](https://github.com/open-telemetry/opentelemetry-java) for the current state of the OpenTelemetry SDK for Java.

## Getting started

<!-- TODO: use #metrics once the "alpha-only" part is no longer there -->
The general setup of OpenTelemetry Java is explained in the official [Getting Started Guide](https://opentelemetry.io/docs/java/manual_instrumentation/).
Using the Metrics API is explained in the [Metrics section](https://opentelemetry.io/docs/java/manual_instrumentation/#metrics-alpha-only).

To include the Dynatrace OpenTelemetry Metrics exporter in a Gradle build, for example,
use the following in your `settings.gradle` and `build.gradle`:

```groovy
// settings.gradle:
sourceControl {
    gitRepository("https://github.com/dynatrace-oss/opentelemetry-metric-java.git") {
        producesModule("com.dynatrace.opentelemetry.metric:dynatrace")
    }
}

// build.gradle:
// use the name of a specific tag from https://github.com/dynatrace-oss/opentelemetry-metric-java/tags
def dynatraceMetricsExporterVersion = "otel-java-v1.2.0"

dependencies {
    implementation("com.dynatrace.opentelemetry.metric:dynatrace:${dynatraceMetricsExporterVersion}")
}
```

Gradle pulls the library in the specified version directly from GitHub and includes it.

To use the library, we first need to create a `DynatraceMetricExporter`.
The `.getDefault()` method returns an instance which attempts to export
to the [local OneAgent endpoint](https://www.dynatrace.com/support/help/how-to-use-dynatrace/metrics/metric-ingestion/ingestion-methods/local-api/).

```java
DynatraceMetricExporter exporter = DynatraceMetricExporter.getDefault();
```

Alternatively, or if no OneAgent is running on the host, the exporter can be set up
using an endpoint URL and an API token with the "ingest metrics" (`metrics.ingest`) permission set.
It is recommended to limit token scope to only this permission.
More information on setting up API access using tokens can be found
[in the documentation](https://www.dynatrace.com/support/help/dynatrace-api/basics/dynatrace-api-authentication/)
and in the [Dynatrace API Token](#dynatrace-api-token) section below.

```java
DynatraceMetricExporter exporter =
    DynatraceMetricExporter.builder()
      .setUrl("https://{your-environment-id}.live.dynatrace.com/api/v2/metrics/ingest")
      .setApiToken({YOUR_API_TOKEN}) // read from environment or config
      .build();
```

After acquiring a `DynatraceMetricExporter` object, it has to be registered using a `MetricReader` of OpenTelemetry:

```java
// Create the PeriodicMetricReaderFactory, passing our exporter and the interval to export:
// The factory is responsible for creating a PeriodicMetricReader instance. The SDK will be doing this.
MetricReaderFactory metricReader = PeriodicMetricReader.create(exporter, Duration.ofMillis(60000));

// Then, we create the MeterProvider, making sure to register our PeriodicMetricReader:
SdkMeterProvider meterProvider = 
    SdkMeterProvider.builder()
    .registerMetricReader(metricReader)
    .buildAndRegisterGlobal();

// OR you can also use a shorter version:
SdkMeterProvider meterProvider =
    SdkMeterProvider.builder()
    .registerMetricReader(PeriodicMetricReader.create(exporter, Duration.ofMillis(60000)))
    .buildAndRegisterGlobal();
```

The interval in which metrics are exported can be set on the `PeriodicMetricReader` (see above).
In the example case above, metrics are exported every 60 seconds.

Once metrics are reported using the Metrics API, data will be exported to Dynatrace:

```java
Meter meter = GlobalMeterProvider.get().get("com.example.my_instrumentation_library", "0.1.0-alpha", "");

LongCounter counter = meter
        .counterBuilder("processed_jobs")
        .setDescription("Processed jobs")
        .setUnit("1")
        .build();

counter.add(123, Attributes.of(stringKey("job-type"), "print-receipt"));
```

A full setup is provided in our [example project](example/src/main/java/com/dynatrace/opentelemetry/metric/example/DynatraceExporterExample.java)

### OpenTelemetry Attributes

#### Typed attributes support

The OpenTelemetry Metrics API for Java supports the concept of
[Attributes](
https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/common/common.md#attributes).
These attributes consist of key-value pairs, where the keys are strings and the values are either primitive types
or array of primitive types.

At the moment, this exporter **only supports string-value attributes**. This means that if attributes of any other type
are used, they will be **ignored** and **only** the string-value attributes are going to be sent to Dynatrace.

Support for typed attributes is being worked on, and this exporter will be updated once that functionality is available.

#### The `Attributes` interface

The way to interact with Attributes is by using the
[Attributes](https://github.com/open-telemetry/opentelemetry-java/blob/main/api/all/src/main/java/io/opentelemetry/api/common/Attributes.java)
interface from the OpenTelemetry API for Java.
You can either use the factory methods `of(...)` or the `AttributesBuilder`. E.g.:

```java
import static io.opentelemetry.api.common.AttributeKey.stringKey;
import io.opentelemetry.api.common.Attributes;

// Using factory 'of' methods
Attributes attributes = Attributes.of(stringKey("attr1"), "value1", stringKey("attr2"), "value2");

// Using the AttributesBuilder:
Attributes attributes =
    Attributes.builder().put("attr1", "value1").put("attr2", "value2").build();
```

The default implementation of `Attributes` given by OpenTelemetry ([ArrayBackedAttributes](https://github.com/open-telemetry/opentelemetry-java/blob/main/api/all/src/main/java/io/opentelemetry/api/common/ArrayBackedAttributes.java))
guarantees that the data is de-duplicated, sorted by keys and no null/empty keys are present.

For this reason, it's recommended that users use the default implementation.
If another implementation is used it **_must_** conform with the `Attributes` interface
otherwise this exporter **cannot be guaranteed** to work properly, as it relies on this behavior.

### Configuration

The exporter allows for configuring the following settings using its builder (`DynatraceMetricExporter.builder()`):

#### Dynatrace API Endpoint

If a OneAgent is installed on the host, it can provide a local endpoint for ingesting metrics without the need for an API token.
The [OneAgent metric API documentation](https://www.dynatrace.com/support/help/how-to-use-dynatrace/metrics/metric-ingestion/ingestion-methods/local-api/)
provides information on how to set up a local OneAgent endpoint.
Using the local API endpoint, the host ID and host name context are automatically added to each metric as dimensions.

If no OneAgent is running on the host or if metrics should be sent to a different endpoint,
the `setUrl` method allows for setting that endpoint.

The metrics ingest endpoint URL looks like:

- `https://{your-environment-id}.live.dynatrace.com/api/v2/metrics/ingest` on SaaS deployments.
- `https://{your-domain}/e/{your-environment-id}/api/v2/metrics/ingest` on managed deployments.

#### Dynatrace API Token

The Dynatrace API token to be used by the exporter is specified using `setApiToken`.
The token could, for example, be read from an environment variable.
It should not be hardcoded into the code, especially if that code is stored in a VCS.

Creating an API token for your Dynatrace environment is described in the
[Dynatrace API documentation](https://www.dynatrace.com/support/help/dynatrace-api/basics/dynatrace-api-authentication/).
The permission required for sending metrics is the `Ingest metrics` (`metrics.ingest`) permission in the **API v2** section,
and it is recommended to limit scope to only this permission:

![API token creation](docs/img/api_token.png)

#### Metric Key Prefix

The `setPrefix` method of the builder specifies an optional prefix, which is prepended to each metric key,
separated by a dot (e.g. a prefix of `<prefix>` and a metric name of `<name>`
will lead to a combined metric name of `<prefix>.<name>`).

In the example, a prefix of `otel.java` is used, which leads to metrics named `otel.java.metric_name`, and allows for clear distinction between metrics from different sources in the Dynatrace metrics UI.

#### Default Dimensions

The `setDefaultDimensions` method can be used to optionally specify a
[Attributes](https://github.com/open-telemetry/opentelemetry-java/blob/main/api/all/src/main/java/io/opentelemetry/api/common/Attributes.java) object,
which will be added as additional dimensions to all data points. The `Attributes` interface represents key-value pairs.

Dimension keys will be normalized, prior to be sent to the server.
Dimensions set on instruments will overwrite default dimensions if they share the same name after normalization.
[OneAgent metadata](#export-oneagent-metadata) will overwrite all dimensions described above,
but it only uses Dynatrace-reserved keys starting with `dt.*`.

The reserved dimension `dt.metrics.source=opentelemetry` will automatically be added to every exported metric when using the exporter.

#### Export OneAgent Metadata

The `setEnrichWithOneAgentMetaData` method on the builder can be used to enable OneAgent metadata export.
If running on a host with a OneAgent, setting this option will export metadata collected by the OneAgent to the Dynatrace endpoint.
When calling the `DynatraceMetricExporter.getDefault()` method, the OneAgent will be assumed as the ingest endpoint, and this option will be set automatically.
If the OneAgent is running and metrics are exported to an explicitly specified endpoint but this method is not called, no OneAgent metadata will be exported.
More information on the underlying OneAgent feature that is used by the exporter can be found in the
[Dynatrace documentation](https://www.dynatrace.com/support/help/how-to-use-dynatrace/metrics/metric-ingestion/ingestion-methods/enrich-metrics/).

### Logging

The log-level for the example project is set to print everything that is logged in the program to the console.
This also prints which messages are sent to the server.
If this is too verbose, set the log level (`.level`) in the [logging.properties](example/src/main/resources/logging.properties) to a higher level, e.g. `INFO` or `WARNING`.
