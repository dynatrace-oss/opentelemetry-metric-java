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

import com.dynatrace.opentelemetry.metric.mint.MintMetricsMessage;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Export metric to Dynatrace. */
public final class DynatraceMetricExporter implements MetricExporter {
  private final URL url;
  private final String apiToken;

  private static final Logger logger = Logger.getLogger(DynatraceMetricExporter.class.getName());

  private DynatraceMetricExporter(URL url, String apiToken) {
    this.url = url;
    this.apiToken = apiToken;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static DynatraceMetricExporter getDefault() {
    Builder builder = new Builder();
    try {
      builder.setUrl(new URL("http://127.0.0.1:14499/metrics/ingest")).build();
    } catch (Exception e) {
      // we can ignore
    }
    return builder.build();
  }

  /**
   * Called by IntervalMetricReader with every collection interval. Could also be called manually.
   *
   * @param metrics is the MetricData collected by all Metric Instruments.
   * @return ResultCode.FAILURE if exporting was not sucessful, ResultCode.SUCCESS otherwise.
   */
  @Override
  public CompletableResultCode export(Collection<MetricData> metrics) {
    MintMetricsMessage metric = MetricAdapter.toMint(metrics);
    return ingestMetricMessage(metric.serialize());
  }

  /**
   * Ingests Metrics via Line Protocol.
   *
   * @param mintMetricsMessage are the already transformed Metrics in a Line Protocol format.
   * @return ResultCode.FAILURE if ingesting was not sucessful, ResultCode.SUCCESS otherwise.
   */
  public CompletableResultCode ingestMetricMessage(String mintMetricsMessage) {
    logger.log(Level.FINEST, "Exporting: {0}", mintMetricsMessage);
    try {
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("POST");
      connection.setRequestProperty("Accept", "*/*; q=0");
      connection.setRequestProperty("Authorization", "Api-Token " + apiToken);
      connection.setRequestProperty("Content-Type", "text/plain; charset=utf-8");
      connection.setDoOutput(true);
      try (final OutputStream outputStream = connection.getOutputStream()) {
        outputStream.write(mintMetricsMessage.getBytes());
      }
      connection.connect();
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
    logger.info("No buffer yet. No-op implementation of flush");
    return CompletableResultCode.ofSuccess();
  }

  @Override
  public void shutdown() {}

  public static class Builder {
    private URL url;
    private String apiToken = null;

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

    public DynatraceMetricExporter build() {
      return new DynatraceMetricExporter(url, apiToken);
    }
  }
}
