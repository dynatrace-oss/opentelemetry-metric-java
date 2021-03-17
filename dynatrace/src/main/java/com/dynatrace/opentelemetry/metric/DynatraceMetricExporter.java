/*
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

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.metrics.common.Labels;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nonnull;

/** Export metric to Dynatrace. */
public final class DynatraceMetricExporter implements MetricExporter {
  private final URL url;
  private final String apiToken;
  private final String prefix;

  private static final Logger logger = Logger.getLogger(DynatraceMetricExporter.class.getName());

  private DynatraceMetricExporter(
      URL url,
      String apiToken,
      String prefix,
      Labels defaultDimensions,
      Boolean enrichWithOneAgentMetaData) {
    this.url = url;
    this.apiToken = apiToken;
    this.prefix = prefix;

    Collection<AbstractMap.SimpleEntry<String, String>> localDimensions = new ArrayList<>();

    if (enrichWithOneAgentMetaData) {
      OneAgentMetadataEnricher enricher = new OneAgentMetadataEnricher(logger);
      localDimensions.addAll(enricher.getDimensionsFromOneAgentMetadata());
    }

    if (defaultDimensions != null) {
      defaultDimensions.forEach(
          (String k, String v) -> {
            localDimensions.add(new AbstractMap.SimpleEntry<>(k, v));
          });
    }

    // add the tags to the MetricAdapter.
    MetricAdapter.getInstance().setTags(localDimensions);
  }

  public static Builder builder() {
    return new Builder();
  }

  /** Returns a default export pointing to a local metric endpoint. */
  public static DynatraceMetricExporter getDefault() {
    Builder builder = new Builder();
    try {
      builder
          .setUrl(new URL("http://127.0.0.1:14499/metrics/ingest"))
              .setPrefix("otel.java")
          .setEnrichWithOneAgentMetaData(true);
    } catch (MalformedURLException e) {
      // we can ignore the URL exception.
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

  @VisibleForTesting
  protected CompletableResultCode export(
      Collection<MetricData> metrics, HttpURLConnection connection) {
    String mintMetricsMessage = MetricAdapter.toMint(metrics).serialize();
    logger.log(Level.FINEST, "Exporting: {0}", mintMetricsMessage);
    try {
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Accept", "*/*; q=0");
      connection.setRequestProperty("Authorization", "Api-Token " + apiToken);
      connection.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
      connection.setDoOutput(true);
      try (final OutputStream outputStream = connection.getOutputStream()) {
        outputStream.write(mintMetricsMessage.getBytes(StandardCharsets.UTF_8));
      }
      int code = connection.getResponseCode();
      if (code != 202) {
        logger.log(Level.WARNING, "Received error code {0} from server", code);
        return CompletableResultCode.ofFailure();
      }
    } catch (Exception e) {
      logger.log(Level.WARNING, "Error while exporting", e);
      return CompletableResultCode.ofFailure();
    }
    return CompletableResultCode.ofSuccess();
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
