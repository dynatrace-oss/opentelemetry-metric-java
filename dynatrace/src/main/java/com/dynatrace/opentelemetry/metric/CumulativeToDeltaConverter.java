package com.dynatrace.opentelemetry.metric;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.opentelemetry.api.common.AttributeType;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * The CumulativeToDeltaConverter provides a way to transform cumulative (total) values to delta
 * values which can be ingested in Dynatrace.
 */
class CumulativeToDeltaConverter {
  private final Cache<String, CacheValue> cache;

  private static class CacheValue {
    private final Number number;
    private final long timeMillis;

    public CacheValue(Number number, long timeMillis) {
      this.number = number;
      this.timeMillis = timeMillis;
    }

    public Number getNumber() {
      return number;
    }

    public long getTimeMillis() {
      return timeMillis;
    }
  }

  /**
   * Create a new converter.
   *
   * @param expireAfter The {@link Duration} after which cache entries will expire, calculated from
   *     the last write operation.
   */
  CumulativeToDeltaConverter(Duration expireAfter) {
    this.cache = CacheBuilder.newBuilder().expireAfterWrite(expireAfter).build();
  }

  /**
   * Convert a total counter to a delta. The identity of a counter is defined by its name and its
   * dimensions.
   *
   * @param metricName The name of the metric.
   * @param point The {@link DoublePointData} object containing the data point.
   * @return The delta to the previous value if there is one with the same identifier in the cache
   *     or the value itself if there is not.
   */
  public Double convertDoubleTotalToDelta(String metricName, DoublePointData point) {
    String identifier = createIdentifier(metricName, point.getAttributes(), "DOUBLE");

    double newValue = point.getValue();
    // reset the map on nan or inf
    if (Double.isNaN(newValue) || Double.isInfinite(newValue)) {
      this.cache.invalidate(identifier);
      return newValue;
    }
    final CacheValue cacheValue = this.cache.getIfPresent(identifier);

    Double deltaValue;
    final long pointTimestamp =
        (point.getEpochNanos() > 0)
            ? TimeUnit.NANOSECONDS.toMillis(point.getEpochNanos())
            : System.currentTimeMillis();
    if (cacheValue == null) {
      // no value in the cache yet or already expired
      deltaValue = null;
    } else {
      if (cacheValue.getTimeMillis() > pointTimestamp) {
        // The current point is older than the one in the cache, so leave the previous value
        // and don't report the current one.
        return null;
      } else {
        // point is newer than the stored data, so calculate delta.
        deltaValue = newValue - cacheValue.getNumber().doubleValue();
      }
    }

    // update the cache
    this.cache.put(identifier, new CacheValue(newValue, pointTimestamp));
    return deltaValue;
  }

  /**
   * Convert a total counter to a delta. The identity of a counter is defined by its name and its
   * dimensions.
   *
   * @param metricName The name of the metric.
   * @param point The {@link LongPointData} object containing the data point.
   * @return The delta to the previous value if there is one with the same identifier in the cache
   *     or the value itself if there is not.
   */
  public Long convertLongTotalToDelta(String metricName, LongPointData point) {
    String identifier = createIdentifier(metricName, point.getAttributes(), "LONG");

    long newValue = point.getValue();
    final CacheValue cacheValue = this.cache.getIfPresent(identifier);

    Long deltaValue;
    final long pointTimestamp =
        (point.getEpochNanos() > 0)
            ? TimeUnit.NANOSECONDS.toMillis(point.getEpochNanos())
            : System.currentTimeMillis();
    if (cacheValue == null) {
      // no value in the cache yet or already expired
      deltaValue = null;
    } else {
      if (cacheValue.getTimeMillis() > pointTimestamp) {
        // The current point is older than the one in the cache, so leave the previous value
        // and don't report the current one.
        return null;
      } else {
        // point is newer than the stored data, so calculate delta.
        deltaValue = newValue - cacheValue.getNumber().longValue();
      }
    }

    // update the cache
    this.cache.put(identifier, new CacheValue(newValue, pointTimestamp));
    return deltaValue;
  }

  // VisibleForTesting
  void reset() {
    this.cache.invalidateAll();
  }

  private static String createIdentifier(String name, Attributes attributes, String type) {
    // (\u001d = ASCII group separator)
    return String.format("%s\u001d%s\u001d%s", name, getSortedAttributesString(attributes), type);
  }

  /**
   * The implementation of the {@link Attributes} interface shipped with OpenTelemetry
   * (ArrayBackedAttributes) guarantees that the elements are sorted by their key, and that no
   * duplicate keys exist. If a different implementation is used instead, it *must* ensure the same
   * behavior, otherwise the behavior of this exporter cannot be guaranteed.
   *
   * @param attributes The attributes of a point.
   * @return A string representation of all attributes.
   */
  private static String getSortedAttributesString(Attributes attributes) {
    if (attributes.isEmpty()) {
      return "";
    }

    StringJoiner joiner = new StringJoiner(",");

    // Attributes are sorted by key once they are "built"
    attributes.forEach(
        (k, v) -> {
          if (k.getType() == AttributeType.STRING) {
            joiner.add(String.format("%s=%s", k.getKey(), v));
          }
        });
    return joiner.toString();
  }
}
