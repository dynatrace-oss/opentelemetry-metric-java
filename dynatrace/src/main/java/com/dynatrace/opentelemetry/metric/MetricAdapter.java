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

import com.dynatrace.opentelemetry.metric.mint.Datapoint;
import com.dynatrace.opentelemetry.metric.mint.Dimension;
import com.dynatrace.opentelemetry.metric.mint.MintMetricsMessage;
import com.google.common.base.Splitter;
import io.opentelemetry.api.common.Labels;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.DoubleSummaryPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.ValueAtPercentile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public final class MetricAdapter {

  private static final Logger logger = Logger.getLogger(MetricAdapter.class.getName());
  private static final List<Dimension> EMPTY = Collections.<Dimension>emptyList();
  private static final Pattern TO_ESCAPE = Pattern.compile("[,=\\s\\\\]");
  private static final Pattern METRICKEY_NOT_ALLOWED = Pattern.compile("[^a-zA-Z0-9:_\\-]");
  private static final Pattern DOESNT_START_WITH_LETTER = Pattern.compile("[^a-zA-Z]");
  private static final Pattern DIMKEY_NOT_ALLOWED = Pattern.compile("[^a-zA-Z0-9:_\\-]");
  private static final Splitter SPLITTER = Splitter.on('.').trimResults().omitEmptyStrings();

  private MetricAdapter() {}

  /**
   * Generates a MintLineProtocolSerializable MintMetricsMessage, that can be ingested to MINT.
   *
   * @param metrics are the OT metrics generated by metric instruments.
   * @return the MintMetricsMessage, that can be serialized to MINT compatible String.
   */
  static MintMetricsMessage toMint(Collection<MetricData> metrics) {
    final MintMetricsMessage.Builder builder = MintMetricsMessage.builder();
    for (MetricData metric : metrics) {
      for (Datapoint datapoint : toDatapoints(metric)) {
        builder.add(datapoint);
      }
    }
    return builder.build();
  }

  /**
   * Converts one metric to a collection of Datapoints.
   *
   * @param metric is the OT MetricData. Contains multiple points if Metrics with same key but
   *     different dimensions are recorded.
   * @return a collection of MINT compatible Datapoints.
   */
  static Collection<Datapoint> toDatapoints(MetricData metric) {
    Collection<Datapoint> datapoints = new ArrayList<>();

    if (metric.isEmpty()) { // necessary due to sometimes an empty MetricData is ingested by OT
      return Collections.emptyList();
    }
    try {
      String metricKeyName = toMintMetricKey(metric.getName());
      addGaugeData(metricKeyName, metric, datapoints);
      addSumData(metricKeyName, metric, datapoints);
      addSummaryData(metricKeyName, metric, datapoints);

      return datapoints;
    } catch (DynatraceExporterException e) {
      logger.warning(e.getMessage());
      return datapoints;
    }
  }

  private static void addSummaryData(
      String metricKeyName, MetricData metric, Collection<Datapoint> datapoints) {
    for (DoubleSummaryPointData point : metric.getDoubleSummaryData().getPoints()) {
      try {
        datapoints.add(generateSummaryPoint(metricKeyName, point));
      } catch (DynatraceExporterException e) {
        logger.warning(e.getMessage());
      }
    }
  }

  static Datapoint generateSummaryPoint(String metricKeyName, DoubleSummaryPointData summaryPoint)
      throws DynatraceExporterException {
    double min = 0.0;
    double max = 0.0;
    double sum = summaryPoint.getSum();
    long count = summaryPoint.getCount();
    List<ValueAtPercentile> valueAtPercentiles = summaryPoint.getPercentileValues();
    for (ValueAtPercentile valueAtPercentile : valueAtPercentiles) {
      // as the lowest possible percentile value is 0.0 and the highest possible is 100.0,
      // comparing the doubles directly should work
      if (valueAtPercentile.getPercentile() == 0.0) {
        min = valueAtPercentile.getValue();
      } else if (valueAtPercentile.getPercentile() == 100.0) {
        max = valueAtPercentile.getValue();
      }
    }
    SummaryStats.DoubleSummaryStat doubleSummaryStat =
        SummaryStats.doubleSummaryStat(min, max, sum, count);

    return Datapoint.create(metricKeyName)
        .timestamp(summaryPoint.getEpochNanos())
        .dimensions(convertLabelsToDimensions(summaryPoint.getLabels()))
        .value(Values.longGauge(doubleSummaryStat))
        .build();
  }

  private static void addSumData(
      String metricKeyName, MetricData metric, Collection<Datapoint> datapoints) {
    boolean isDeltaDouble =
        metric.getDoubleSumData().getAggregationTemporality() == AggregationTemporality.DELTA;
    for (DoublePointData data : metric.getDoubleSumData().getPoints()) {
      try {
        Datapoint p =
            Datapoint.create(metricKeyName)
                .timestamp(data.getEpochNanos())
                .dimensions(convertLabelsToDimensions(data.getLabels()))
                .value(Values.doubleCount(data.getValue(), /* isDelta= */ isDeltaDouble))
                .build();
        datapoints.add(p);
      } catch (DynatraceExporterException e) {
        logger.warning(e.getMessage());
      }
    }

    boolean isDeltaLong =
        metric.getLongSumData().getAggregationTemporality() == AggregationTemporality.DELTA;
    for (LongPointData data : metric.getLongSumData().getPoints()) {
      try {
        Datapoint p =
            Datapoint.create(metricKeyName)
                .timestamp(data.getEpochNanos())
                .dimensions(convertLabelsToDimensions(data.getLabels()))
                .value(Values.longCount(data.getValue(), /* isDelta= */ isDeltaLong))
                .build();
        datapoints.add(p);
      } catch (DynatraceExporterException e) {
        logger.warning(e.getMessage());
      }
    }
  }

  private static void addGaugeData(
      String metricKeyName, MetricData metric, Collection<Datapoint> datapoints) {

    for (DoublePointData data : metric.getDoubleGaugeData().getPoints()) {
      try {
        Datapoint p =
            Datapoint.create(metricKeyName)
                .timestamp(data.getEpochNanos())
                .dimensions(convertLabelsToDimensions(data.getLabels()))
                .value(Values.doubleCount(data.getValue(), /* isDelta= */ true))
                .build();
        datapoints.add(p);
      } catch (DynatraceExporterException e) {
        logger.warning(e.getMessage());
      }
    }

    for (LongPointData data : metric.getLongGaugeData().getPoints()) {
      try {
        Datapoint p =
            Datapoint.create(metricKeyName)
                .timestamp(data.getEpochNanos())
                .dimensions(convertLabelsToDimensions(data.getLabels()))
                .value(Values.longCount(data.getValue(), /* isDelta= */ true))
                .build();
        datapoints.add(p);
      } catch (DynatraceExporterException e) {
        logger.warning(e.getMessage());
      }
    }
  }

  /**
   * Converts a Map of key-value labels to a List of MINT compatible Dimensions.
   *
   * @param labels are the labels generated by OT.
   * @return all sanitized Dimensions. List is empty, if a key-value pair doesn't match the MINT
   *     requirements
   */
  private static List<Dimension> convertLabelsToDimensions(Labels labels)
      throws DynatraceExporterException {
    final List<Dimension> dimensions = new ArrayList<>(labels.size());
    labels.forEach(
        new BiConsumer<String, String>() {
          @Override
          public void accept(String k, String v) {
            dimensions.add(toMintDimension(k, v));
          }
        });

    if (dimensions.size() != labels.size()) {
      return EMPTY;
    }
    return Collections.unmodifiableList(dimensions);
  }

  /**
   * Sanitizes MINT metric keys regarding correct grammar.
   *
   * @param metricKey is the metric key, that needs to be sanitized.
   * @return the sanitized metric key.
   * @throws DynatraceExporterException if the metric key is null, part of the metric key (split by
   *     '.') starts with non-letter character or is empty. Further cases for returning the
   *     exception include a not allowed suffix by MINT or if the whole metric key exceeds the
   *     allowed length limits.
   */
  static String toMintMetricKey(String metricKey) throws DynatraceExporterException {
    StringBuilder builder = new StringBuilder();
    for (String metricKeySection : SPLITTER.split(metricKey)) {
      if (builder.length() != 0) {
        builder.append(".");
      }

      if (DOESNT_START_WITH_LETTER.matcher(String.valueOf(metricKeySection.charAt(0))).matches()) {
        throw new DynatraceExporterException(
            "Metric key section "
                + trimForLogOutput(metricKeySection)
                + " of key "
                + trimForLogOutput(metricKey)
                + " starts with illegal character. Discarding line.");
      }
      char[] chars = metricKeySection.toCharArray();

      for (int i = 1; i < chars.length; i++) {
        if (METRICKEY_NOT_ALLOWED.matcher(String.valueOf(chars[i])).matches()) {
          chars[i] = '_';
        }
      }
      builder.append(chars);
    }
    String sanitizedMetricKey = builder.toString();

    if (!sanitizedMetricKey.equals(metricKey)) {
      logger.info(
          "Sanitized OT metric key "
              + trimForLogOutput(metricKey)
              + " to ingested MINT metric key "
              + trimForLogOutput(sanitizedMetricKey)
              + ".");
    }
    return sanitizedMetricKey;
  }

  /**
   * Convert a key-value pair to a MINT-compatible Dimension.
   *
   * @param key is the key as String.
   * @param value is the value as String.
   * @return a MINT-compatible Dimension (sanitized key and value).
   * @throws DynatraceExporterException if an error occured during sanitizing the Strings.
   */
  static Dimension toMintDimension(String key, String value) throws DynatraceExporterException {
    return Dimension.create(toMintDimensionKey(key), toMintDimensionValue(value));
  }

  /**
   * Sanitizes the dimension key to be compatible with MINT.
   *
   * @param dimensionKey is the dimension key as String.
   * @return the sanitized dimension key.
   * @throws DynatraceExporterException if the dimension key is null, a section of it is empty or
   *     starts with an illegal character (sections split by '.') or the key exceeds the MINT
   *     specification limits.
   */
  static String toMintDimensionKey(String dimensionKey) throws DynatraceExporterException {
    if (dimensionKey.startsWith(".")
        || dimensionKey.endsWith(
            ".")) { // required because String.split() doesn't work correctly if first or last
      // letter is the one to split by
      throw new DynatraceExporterException(
          "Dimension key "
              + trimForLogOutput(dimensionKey)
              + " contains empty section. Discarding line.");
    } else if (dimensionKey.length() < 1) {
      throw new DynatraceExporterException(
          "Dimension key "
              + trimForLogOutput(dimensionKey)
              + " has less than 1 character. Discarding line");
    }
    StringBuilder dimKeyBuilder = new StringBuilder();

    for (String dimKeySection : SPLITTER.split(dimensionKey)) {
      if (dimKeyBuilder.length() != 0) {
        dimKeyBuilder.append(".");
      }
      if (DOESNT_START_WITH_LETTER.matcher(String.valueOf(dimKeySection.charAt(0))).matches()) {
        throw new DynatraceExporterException(
            "Dimension key section "
                + trimForLogOutput(dimKeySection)
                + " of key "
                + trimForLogOutput(dimensionKey)
                + " starts with illegal character. Discarding line.");
      }

      // the following replaceAll are expensive operations and may have to be removed
      char[] chars =
          dimKeySection
              .replaceAll("[^\\x00-\\x7F]", "_") // replacing non-ASCII chars
              .replaceAll("[\\p{Cntrl}&&[^\r\n\t]]", "_") // replacing ASCII control chars
              .replaceAll(
                  "\\p{C}",
                  "_") // replacing non-printable chars from Unicode (evaluate if necessary!)
              .toCharArray();

      for (int i = 0; i < chars.length; i++) {
        if (DIMKEY_NOT_ALLOWED.matcher(String.valueOf(chars[i])).matches()) {
          chars[i] = '_';
        } else if (String.valueOf(chars[i]).matches("[A-Z]")) {
          chars[i] = Character.toLowerCase(chars[i]);
        }
      }
      dimKeyBuilder.append(chars);
    }

    if (!dimKeyBuilder.toString().equals(dimensionKey)) {
      logger.info(
          "Sanitized OT dimension key "
              + trimForLogOutput(dimensionKey)
              + " to ingested MINT dimension key "
              + trimForLogOutput(dimKeyBuilder.toString())
              + ".");
    }
    return dimKeyBuilder.toString();
  }

  /**
   * Sanitizes the dimension value to be compatible with MINT.
   *
   * @param dimensionValue is the dimension value as String.
   * @return the sanitized dimension value.
   * @throws DynatraceExporterException if dimension value is null or exceeds the MINT specification
   *     limits.
   */
  static String toMintDimensionValue(String dimensionValue) throws DynatraceExporterException {
    if (dimensionValue.length() < 1) {
      throw new DynatraceExporterException(
          "Dimension value "
              + trimForLogOutput(dimensionValue)
              + " does not have at least a character. Discarding the line.");
    } else {
      String escapedString = dimensionValue.replace("\\", "\\\\").replace("\"", "\\\"");
      if (!dimensionValue.equals(escapedString)) {
        logger.info(
            "Sanitized OT dimension value "
                + trimForLogOutput(dimensionValue)
                + " to ingested MINT dimension value "
                + trimForLogOutput(escapedString)
                + ".");
      }
      return escapedString;
    }
  }

  /**
   * Escapes a String with '"' if it is required by MINT.
   *
   * @param value is the input String.
   * @return '"'value'"' if value contains specific char that requires escaping, else the unmodified
   *     value.
   */
  public static String escapeIfNecessary(String value) {
    return TO_ESCAPE.matcher(value).find() ? "\"" + value + "\"" : value;
  }

  /**
   * Trims a String to it's first characters (determined by config) for a more overviewable log
   * output.
   *
   * @param value is the input String.
   * @return the trimmed String.
   */
  private static String trimForLogOutput(String value) {
    return value;
  }
}
