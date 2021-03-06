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

import com.dynatrace.metric.util.Dimension;
import com.dynatrace.metric.util.DimensionList;
import com.dynatrace.metric.util.DynatraceMetricApiConstants;
import com.dynatrace.metric.util.MetricBuilderFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import io.opentelemetry.api.metrics.common.Labels;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

/** Export metric to Dynatrace. */
public final class DynatraceMetricExporter implements MetricExporter {
  private final URL url;
  private final String apiToken;
  private final Serializer serializer;

  private static final Logger logger = Logger.getLogger(DynatraceMetricExporter.class.getName());
  private static final List<Dimension> staticDimensions =
      new ArrayList<Dimension>() {
        {
          add(Dimension.create("dt.metrics.source", "opentelemetry"));
        }
      };

  private static final Pattern EXTRACT_LINES_OK = Pattern.compile("\"linesOk\":\\s?(\\d+)");
  private static final Pattern EXTRACT_LINES_INVALID =
      Pattern.compile("\"linesInvalid\":\\s?(\\d+)");
  private static final Pattern RETURNED_ERROR_FIELD_IS_NULL = Pattern.compile("\"error\":\\s?null");

  private DynatraceMetricExporter(
      URL url,
      String apiToken,
      String prefix,
      Labels defaultDimensions,
      Boolean enrichWithOneAgentMetaData) {
    this.url = url;
    this.apiToken = apiToken;

    MetricBuilderFactory.MetricBuilderFactoryBuilder builder = MetricBuilderFactory.builder();

    if (!Strings.isNullOrEmpty(prefix)) {
      builder = builder.withPrefix(prefix);
    }

    if (enrichWithOneAgentMetaData) {
      builder = builder.withOneAgentMetadata();
    }

    List<Dimension> dimensions;
    if (defaultDimensions != null) {
      dimensions = Serializer.toListOfDimensions(defaultDimensions);
    } else {
      dimensions = new ArrayList<>();
    }

    dimensions.addAll(staticDimensions);
    builder.withDefaultDimensions(DimensionList.fromCollection(dimensions));

    serializer = new Serializer(builder.build());
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Returns a default export pointing to a local metric endpoint. */
  public static DynatraceMetricExporter getDefault() {
    Builder builder = new Builder();
    try {
      builder
          .setUrl(new URL(DynatraceMetricApiConstants.getDefaultOneAgentEndpoint()))
          .setEnrichWithOneAgentMetaData(true);
    } catch (MalformedURLException e) {
      // We can ignore the URL exception since we know we are passing a valid URL.
    }
    return builder.build();
  }

  /**
   * Called by IntervalMetricReader with every collection interval. Could also be called manually.
   *
   * @param metrics is the MetricData collected by all Metric Instruments.
   * @return ResultCode.FAILURE if exporting was not successful, ResultCode.SUCCESS otherwise.
   */
  @Override
  public CompletableResultCode export(@Nonnull Collection<MetricData> metrics) {
    HttpURLConnection connection;
    try {
      connection = (HttpURLConnection) url.openConnection();
    } catch (Exception e) {
      logger.log(Level.WARNING, "Error while exporting", e);
      return CompletableResultCode.ofFailure();
    }

    return export(metrics, connection);
  }

  private List<String> serializeToMetricLines(Collection<MetricData> metrics) {
    ArrayList<String> metricLines = new ArrayList<>();
    for (MetricData metric : metrics) {
      boolean isDelta;
      switch (metric.getType()) {
        case LONG_GAUGE:
          metricLines.addAll(serializer.createLongGaugeLines(metric));
          break;
        case LONG_SUM:
          isDelta =
              metric.getLongSumData().getAggregationTemporality() == AggregationTemporality.DELTA;
          metricLines.addAll(serializer.createLongSumLines(metric, isDelta));
          break;
        case DOUBLE_GAUGE:
          metricLines.addAll(serializer.createDoubleGaugeLines(metric));
          break;
        case DOUBLE_SUM:
          isDelta =
              metric.getDoubleSumData().getAggregationTemporality() == AggregationTemporality.DELTA;
          metricLines.addAll(serializer.createDoubleSumLines(metric, isDelta));
          break;
        case SUMMARY:
          metricLines.addAll(serializer.createDoubleSummaryLines(metric));
          break;
        case HISTOGRAM:
          metricLines.addAll(serializer.createDoubleHistogramLines(metric));
          break;
        default:
          logger.warning(
              String.format(
                  "Tried to serialize metric of type %s. The Dynatrace metrics exporter does not handle metrics of that type at the time.",
                  metric.getType().toString()));
          break;
      }
    }

    return metricLines;
  }

  @VisibleForTesting
  protected CompletableResultCode export(
      Collection<MetricData> metrics, HttpURLConnection connection) {

    List<String> metricLines = serializeToMetricLines(metrics);
    for (List<String> partition :
        Lists.partition(metricLines, DynatraceMetricApiConstants.getPayloadLinesLimit())) {
      CompletableResultCode resultCode;
      String joinedMetricLines = Joiner.on('\n').join(partition);

      if (logger.isLoggable(Level.FINER)) {
        logger.finer(String.format("Exporting metrics:\n%s", joinedMetricLines));
      }
      try {
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Accept", "*/*; q=0");
        if (this.apiToken != null) {
          connection.setRequestProperty("Authorization", "Api-Token " + apiToken);
        }
        connection.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
        connection.setRequestProperty("User-Agent", "opentelemetry-metric-java");
        connection.setDoOutput(true);
        try (final OutputStream outputStream = connection.getOutputStream()) {
          outputStream.write(joinedMetricLines.getBytes(StandardCharsets.UTF_8));
        }
        int code = connection.getResponseCode();
        if (code < 400) {
          String response =
              CharStreams.toString(
                  new InputStreamReader(connection.getInputStream(), Charsets.UTF_8));
          resultCode = handleSuccess(code, metricLines.size(), response);
        } else {
          String response =
              CharStreams.toString(
                  new InputStreamReader(connection.getErrorStream(), Charsets.UTF_8));
          logger.log(
              Level.WARNING,
              String.format(
                  "Error while exporting. Status code: %d; Response: %s", code, response));
          resultCode = CompletableResultCode.ofFailure();
        }
      } catch (Exception e) {
        logger.log(Level.WARNING, "Error while exporting", e);
        resultCode = CompletableResultCode.ofFailure();
      }
      if (!resultCode.isSuccess()) {
        return resultCode;
      }
    }
    return CompletableResultCode.ofSuccess();
  }

  private CompletableResultCode handleSuccess(int code, int totalLines, String response) {
    if (code == 202) {
      if (RETURNED_ERROR_FIELD_IS_NULL.matcher(response).find()) {
        Matcher linesOkMatchResult = EXTRACT_LINES_OK.matcher(response);
        Matcher linesInvalidMatchResult = EXTRACT_LINES_INVALID.matcher(response);
        if (linesOkMatchResult.find() && linesInvalidMatchResult.find()) {
          if (logger.isLoggable(Level.FINE)) {
            logger.fine(
                String.format(
                    "Sent %d metric lines, linesOk: %s linesInvalid: %s",
                    totalLines, linesOkMatchResult.group(1), linesInvalidMatchResult.group(1)));
          }
          return CompletableResultCode.ofSuccess();
        }
      }
      logger.warning(String.format("could not parse response: %s", response));
    } else {
      // common pitfall if URI is supplied in v1 format (without endpoint path)
      logger.warning(
          String.format(
              "Expected status code 202, got %d. Did you specify the ingest path (e. g. /api/v2/metrics/ingest)?",
              code));
    }
    return CompletableResultCode.ofFailure();
  }

  @Override
  public CompletableResultCode flush() {
    return CompletableResultCode.ofSuccess();
  }

  @Override
  public CompletableResultCode shutdown() {
    return CompletableResultCode.ofSuccess();
  }

  public static class Builder {
    private URL url;
    private String apiToken = null;
    private Boolean enrichWithOneAgentMetaData = false;
    private String prefix;
    private Labels defaultDimensions;

    public Builder setUrl(String url) throws MalformedURLException {
      this.url = new URL(url);
      return this;
    }

    public Builder setUrl(URL url) {
      this.url = url;
      return this;
    }

    public Builder setApiToken(String apiToken) {
      this.apiToken = apiToken;
      return this;
    }

    public Builder setEnrichWithOneAgentMetaData(Boolean enrich) {
      this.enrichWithOneAgentMetaData = enrich;
      return this;
    }

    public Builder setPrefix(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public Builder setDefaultDimensions(Labels defaultDimensions) {
      this.defaultDimensions = defaultDimensions;
      return this;
    }

    public DynatraceMetricExporter build() {
      return new DynatraceMetricExporter(
          url, apiToken, prefix, defaultDimensions, enrichWithOneAgentMetaData);
    }
  }
}
