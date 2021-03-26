package com.dynatrace.opentelemetry.metric;

import com.dynatrace.metric.util.*;
import io.opentelemetry.api.metrics.common.Labels;
import io.opentelemetry.sdk.metrics.data.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

final class Serializer {
  private static final Logger logger = Logger.getLogger(Serializer.class.getName());
  private static final double PERCENTILE_PRECISION = 0.0000001;

  private final MetricBuilderFactory builderFactory;

  Serializer(MetricBuilderFactory builderFactory) {
    this.builderFactory = builderFactory;
  }

  Metric.Builder createMetricBuilder(MetricData metric, PointData point) {
    return builderFactory
        .newMetricBuilder(metric.getName())
        .setDimensions(fromLabels(point.getLabels()))
        .setTimestamp(Instant.ofEpochMilli(TimeUnit.NANOSECONDS.toMillis(point.getEpochNanos())));
  }

  static DimensionList fromLabels(Labels labels) {
    ArrayList<Dimension> dimensions = new ArrayList<>(labels.size());
    labels.forEach((k, v) -> dimensions.add(Dimension.create(k, v)));
    return DimensionList.fromCollection(dimensions);
  }

  List<String> createLongSumLines(MetricData metric, boolean isDelta) {
    List<String> lines = new ArrayList<>();
    for (LongPointData point : metric.getLongSumData().getPoints()) {
      try {
        Metric.Builder builder = createMetricBuilder(metric, point);

        if (isDelta) {
          builder.setLongAbsoluteCounterValue(point.getValue());
        } else {
          builder.setLongCounterValue(point.getValue());
        }

        lines.add(builder.serialize());
      } catch (MetricException me) {
        logger.warning(
            String.format(
                "could not create metric line for data point with name %s.", metric.getName()));
      }
    }
    return lines;
  }

  List<String> createLongGaugeLines(MetricData metric) {
    List<String> lines = new ArrayList<>();
    for (LongPointData point : metric.getLongGaugeData().getPoints()) {
      try {
        lines.add(
            createMetricBuilder(metric, point).setLongGaugeValue(point.getValue()).serialize());
      } catch (MetricException me) {
        logger.warning(
            String.format(
                "could not create metric line for data point with name %s.", metric.getName()));
      }
    }
    return lines;
  }

  public List<String> createDoubleGaugeLines(MetricData metric) {
    List<String> lines = new ArrayList<>();
    for (DoublePointData point : metric.getDoubleGaugeData().getPoints()) {
      try {
        lines.add(
            createMetricBuilder(metric, point).setDoubleGaugeValue(point.getValue()).serialize());
      } catch (MetricException me) {
        logger.warning(
            String.format(
                "could not create metric line for data point with name %s.", metric.getName()));
      }
    }
    return lines;
  }

  List<String> createDoubleSumLines(MetricData metric, boolean isDelta) {
    List<String> lines = new ArrayList<>();
    for (DoublePointData point : metric.getDoubleSumData().getPoints()) {
      try {
        Metric.Builder builder = createMetricBuilder(metric, point);

        if (isDelta) {
          builder.setDoubleAbsoluteCounterValue(point.getValue());
        } else {
          builder.setDoubleCounterValue(point.getValue());
        }

        lines.add(builder.serialize());
      } catch (MetricException me) {
        logger.warning(
            String.format(
                "could not create metric line for data point with name %s.", metric.getName()));
      }
    }
    return lines;
  }

  public List<String> createDoubleSummaryLines(MetricData metric) {
    List<String> lines = new ArrayList<>();
    for (DoubleSummaryPointData point : metric.getDoubleSummaryData().getPoints()) {
      double min = .0;
      double max = .0;
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

      try {
        lines.add(
            createMetricBuilder(metric, point)
                .setDoubleSummaryValue(min, max, sum, count)
                .serialize());
      } catch (MetricException e) {
        logger.warning(
            String.format(
                "could not create metric line for data point with name %s.", metric.getName()));
      }
    }
    return lines;
  }
}
