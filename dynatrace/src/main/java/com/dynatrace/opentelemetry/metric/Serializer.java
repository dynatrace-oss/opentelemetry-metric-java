/**
 * Copyright 2021 Dynatrace LLC
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

import com.dynatrace.metric.util.*;
import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.api.metrics.common.Labels;
import io.opentelemetry.sdk.metrics.data.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

final class Serializer {
  private static final Logger logger = Logger.getLogger(Serializer.class.getName());
  // the precision used to identify whether a percentile is the 0% (min) or 100% (max) percentile.
  private static final double PERCENTILE_PRECISION = 0.0001;
  private static final String TEMPLATE_ERR_METRIC_LINE =
      "Could not create metric line for data point with name %s (%s).";

  private final MetricBuilderFactory builderFactory;

  Serializer(MetricBuilderFactory builderFactory) {
    this.builderFactory = builderFactory;
  }

  private Metric.Builder createMetricBuilder(MetricData metric, PointData point) {
    return builderFactory
        .newMetricBuilder(metric.getName())
        .setDimensions(fromLabels(point.getLabels()))
        .setTimestamp(Instant.ofEpochMilli(TimeUnit.NANOSECONDS.toMillis(point.getEpochNanos())));
  }

  @VisibleForTesting
  static DimensionList fromLabels(Labels labels) {
    ArrayList<Dimension> dimensions = new ArrayList<>(labels.size());
    labels.forEach((k, v) -> dimensions.add(Dimension.create(k, v)));
    return DimensionList.fromCollection(dimensions);
  }

  @VisibleForTesting
  List<String> createLongSumLines(MetricData metric, boolean isDelta) {
    List<String> lines = new ArrayList<>();
    for (LongPointData point : metric.getLongSumData().getPoints()) {
      try {
        Metric.Builder builder = createMetricBuilder(metric, point);

        if (isDelta) {
          builder.setLongCounterValueDelta(point.getValue());
        } else {
          builder.setLongCounterValueTotal(point.getValue());
        }

        lines.add(builder.serialize());
      } catch (MetricException me) {
        logger.warning(String.format(TEMPLATE_ERR_METRIC_LINE, metric.getName(), me.getMessage()));
      }
    }
    return lines;
  }

  @VisibleForTesting
  List<String> createLongGaugeLines(MetricData metric) {
    List<String> lines = new ArrayList<>();
    for (LongPointData point : metric.getLongGaugeData().getPoints()) {
      try {
        lines.add(
            createMetricBuilder(metric, point).setLongGaugeValue(point.getValue()).serialize());
      } catch (MetricException me) {
        logger.warning(String.format(TEMPLATE_ERR_METRIC_LINE, metric.getName(), me.getMessage()));
      }
    }
    return lines;
  }

  @VisibleForTesting
  List<String> createDoubleGaugeLines(MetricData metric) {
    List<String> lines = new ArrayList<>();
    for (DoublePointData point : metric.getDoubleGaugeData().getPoints()) {
      try {
        lines.add(
            createMetricBuilder(metric, point).setDoubleGaugeValue(point.getValue()).serialize());
      } catch (MetricException me) {
        logger.warning(String.format(TEMPLATE_ERR_METRIC_LINE, metric.getName(), me.getMessage()));
      }
    }
    return lines;
  }

  @VisibleForTesting
  List<String> createDoubleSumLines(MetricData metric, boolean isDelta) {
    List<String> lines = new ArrayList<>();
    for (DoublePointData point : metric.getDoubleSumData().getPoints()) {
      try {
        Metric.Builder builder = createMetricBuilder(metric, point);

        if (isDelta) {
          builder.setDoubleCounterValueDelta(point.getValue());
        } else {
          builder.setDoubleCounterValueTotal(point.getValue());
        }

        lines.add(builder.serialize());
      } catch (MetricException me) {
        logger.warning(String.format(TEMPLATE_ERR_METRIC_LINE, metric.getName(), me.getMessage()));
      }
    }
    return lines;
  }

  @VisibleForTesting
  List<String> createDoubleSummaryLines(MetricData metric) {
    List<String> lines = new ArrayList<>();
    for (DoubleSummaryPointData point : metric.getDoubleSummaryData().getPoints()) {
      double min = Double.NaN;
      double max = Double.NaN;
      double sum = point.getSum();
      long count = point.getCount();

      List<ValueAtPercentile> percentileValues = point.getPercentileValues();
      for (ValueAtPercentile percentileValue : percentileValues) {
        if (Math.abs(percentileValue.getPercentile() - 0.0) < PERCENTILE_PRECISION) {
          min = percentileValue.getValue();
        } else if (Math.abs(percentileValue.getPercentile() - 100.0) < PERCENTILE_PRECISION) {
          max = percentileValue.getValue();
        }
      }
      if (Double.isNaN(min) || Double.isNaN(max)) {
        logger.warning(
            "The min and/or max value could not be retrieved. This happens if the 0% and 100% quantile are not set for the summary.");
      }

      try {
        lines.add(
            createMetricBuilder(metric, point)
                .setDoubleSummaryValue(min, max, sum, count)
                .serialize());
      } catch (MetricException me) {
        logger.warning(String.format(TEMPLATE_ERR_METRIC_LINE, metric.getName(), me.getMessage()));
      }
    }
    return lines;
  }

  @VisibleForTesting
  List<String> createDoubleHistogramLines(MetricData metric) {
    List<String> lines = new ArrayList<>();
    for (DoubleHistogramPointData point : metric.getDoubleHistogramData().getPoints()) {
      double min = Double.POSITIVE_INFINITY;
      double max = Double.NEGATIVE_INFINITY;
      double sum = point.getSum();
      long count = point.getCount();

      for (Double boundary : point.getBoundaries()) {
        if (boundary < min && boundary > Double.NEGATIVE_INFINITY) {
          min = boundary;
        }

        if (boundary > max && boundary < Double.POSITIVE_INFINITY) {
          max = boundary;
        }
      }
      try {
        lines.add(
            createMetricBuilder(metric, point)
                .setDoubleSummaryValue(min, max, sum, count)
                .serialize());
      } catch (MetricException me) {
        logger.warning(String.format(TEMPLATE_ERR_METRIC_LINE, metric.getName(), me.getMessage()));
      }
    }
    return lines;
  }
}
