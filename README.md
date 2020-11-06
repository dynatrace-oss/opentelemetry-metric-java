# opentelemetry-metric-java

## Usage

First, create a `DynatraceMetricExporter`:

```java
DynatraceMetricExporter exporter =
    DynatraceMetricExporter.builder()
      .setApiToken({API_TOKEN})
      .setUrl({INGEST_URL})
      .build();
```

If the exporter is running together with the OneAgent, it can be initialized with:

```java
DynatraceMetricExporter exporter = DynatraceMetricExporter.getDeault();
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

For how to register metrics, refer to the
[OpenTelemetry Java Quick Start guide](https://github.com/open-telemetry/opentelemetry-java/blob/master/QUICKSTART.md#metrics).