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
import java.time.Duration;
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
  private final CumulativeToDeltaConverter deltaConverter;

  Serializer(MetricBuilderFactory builderFactory) {
    this.builderFactory = builderFactory;
    this.deltaConverter = new CumulativeToDeltaConverter(Duration.ofMinutes(15));
  }

  private Metric.Builder createMetricBuilder(MetricData metric, PointData point) {
    Metric.Builder builder =
        builderFactory
            .newMetricBuilder(metric.getName())
            .setDimensions(fromLabels(point.getLabels()));
    long epochNanos = point.getEpochNanos();
    // Only set a timestamp if it is available for the PointData.
    // If it is missing, the server will use the current time at ingest.
    if (epochNanos > 0) {
      builder.setTimestamp(Instant.ofEpochMilli(TimeUnit.NANOSECONDS.toMillis(epochNanos)));
    }
    return builder;
  }

  static List<Dimension> toListOfDimensions(Labels labels) {
    ArrayList<Dimension> dimensions = new ArrayList<>(labels.size());
    labels.forEach((k, v) -> dimensions.add(Dimension.create(k, v)));
    return dimensions;
  }

  static DimensionList fromLabels(Labels labels) {
    return DimensionList.fromCollection(toListOfDimensions(labels));
  }

  @VisibleForTesting
  List<String> createLongSumLines(MetricData metric, boolean isDelta) {
    List<String> lines = new ArrayList<>();
    for (LongPointData point : metric.getLongSumData().getPoints()) {
      try {
        Metric.Builder builder = createMetricBuilder(metric, point);

        if (isDelta) {
          builder.setLongCounterValueDelta(point.getValue());
          lines.add(builder.serialize());
        } else {
          Long delta = deltaConverter.convertLongTotalToDelta(metric.getName(), point);
          if (delta != null) {
            builder.setLongCounterValueDelta(delta);
            lines.add(builder.serialize());
          } else {
            logger.finest(
                "Skipping delta line creation, since the value was not present in the cache before this value.");
          }
        }
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
          lines.add(builder.serialize());
        } else {
          Double delta = deltaConverter.convertDoubleTotalToDelta(metric.getName(), point);
          if (delta != null) {
            builder.setDoubleCounterValueDelta(delta);
            lines.add(builder.serialize());
          } else {
            logger.finest(
                "Skipping delta line creation, since the value was not present in the cache before this value.");
          }
        }

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
      double min = getMinFromBoundaries(point);
      double max = getMaxFromBoundaries(point);
      double sum = point.getSum();
      long count = point.getCount();

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
  static double getMinFromBoundaries(DoubleHistogramPointData pointData) {
    if (pointData.getCounts().size() == 1) {
      // In this case, only one bucket exists: (-Inf, Inf). If there were any boundaries, there
      // would be more counts.
      if (pointData.getCounts().get(0) > 0) {
        // in case the single bucket contains something, use the mean as min.
        return pointData.getSum() / pointData.getCount();
      }
      // otherwise the histogram has no data. Use the sum as the min and max, respectively.
      return pointData.getSum();
    }

    for (int i = 0; i < pointData.getCounts().size(); i++) {
      if (pointData.getCounts().get(i) > 0) {
        // the current bucket contains something.
        if (i == 0) {
          // If we are in the first bucket, use the upper bound (which is the lowest specified bound
          // overall) otherwise this would be -Inf, which is not allowed. This is not quite correct,
          // but the best approximation we can get at this point. This might however lead to a min
          // that is bigger than the sum, therefore we return the min of the sum and the lowest
          // bound.
          // Choose the minimum of the following three:
          // - The lowest boundary
          // - The sum (smallest if there are multiple negative measurements smaller than the lowest
          // boundary)
          // - The average in the bucket (smallest if there are multiple positive measurements
          // smaller than the lowest boundary)
          return Math.min(
              Math.min(pointData.getBoundaries().get(i), pointData.getSum()),
              pointData.getSum() / pointData.getCount());
        }
        return pointData.getBoundaries().get(i - 1);
      }
    }

    // there are no counts > 0, so calculating a mean would result in a division by 0. By returning
    // the sum, we can let the backend decide what to do with the value (with a count of 0)
    return pointData.getSum();
  }

  @VisibleForTesting
  static double getMaxFromBoundaries(DoubleHistogramPointData pointData) {
    // see getMinFromBoundaries for a very similar method that is annotated.
    if (pointData.getCounts().size() == 1) {
      if (pointData.getCounts().get(0) > 0) {
        return pointData.getSum() / pointData.getCount();
      }
      return pointData.getSum();
    }

    int lastElemIdx = pointData.getCounts().size() - 1;
    // loop over counts in reverse
    for (int i = lastElemIdx; i >= 0; i--) {
      if (pointData.getCounts().get(i) > 0) {
        if (i == lastElemIdx) {
          // use the last bound in the bounds array. This can only be the case if there is a count >
          // 0 in the last bucket (lastBound, Inf), therefore, the bound has to be smaller than the
          // actual maximum value, which in turn ensures that the sum is larger than the bound we
          // use as max here.
          return pointData.getBoundaries().get(i - 1);
        }
        // in any bucket except the last, make sure the sum is greater than or equal to the max,
        // otherwise report the sum.
        return Math.min(pointData.getBoundaries().get(i), pointData.getSum());
      }
    }

    return pointData.getSum();
  }
}
