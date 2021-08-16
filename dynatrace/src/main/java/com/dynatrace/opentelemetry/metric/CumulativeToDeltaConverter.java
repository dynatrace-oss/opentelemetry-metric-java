package com.dynatrace.opentelemetry.metric;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Duration;

/**
 * The CumulativeToDeltaConverter provides a way to transform cumulative (total) values to delta
 * values which can be ingested in Dynatrace.
 */
class CumulativeToDeltaConverter {
  private final Cache<String, Number> cache;

  /**
   * Create a new converter.
   *
   * @param expireAfter The duration after which cache entries will expire, calculated from the last
   *     write operation.
   */
  CumulativeToDeltaConverter(Duration expireAfter) {
    this.cache = CacheBuilder.newBuilder().expireAfterWrite(expireAfter).build();
  }

  /**
   * Calculate the delta to the previous timestamp, or return the passed new value if the identifier
   * is missing from the cache.
   *
   * @param identifier The identifier stored in the map. Use {@link
   *     com.dynatrace.metric.util.MetricBuilderIdentifierCreator the
   *     MetricBuilderIdentifierCreator} to create the identifier from a metric builder.
   * @param newValue The new (cumulative) value which should be stored in the cache, and from which
   *                 the delta version is calculated.
   * @return The difference to the previous version, if there was already a value for the 
   * identifier, or the newValue parameter otherwise.
   */
  public Double convertTotalCounterToDeltaAndUpdateCache(String identifier, Double newValue) {
    @Nullable Number oldValue = this.cache.getIfPresent(identifier);
    Double deltaValue;
    if (null == oldValue) {
      deltaValue = newValue;
    } else {
      deltaValue = newValue - oldValue.doubleValue();
    }
    this.cache.put(identifier, newValue);
    return deltaValue;
  }

  /**
   * Calculate the delta to the previous timestamp, or return the passed new value if the identifier
   * is missing from the cache.
   *
   * @param identifier The identifier stored in the map. Use {@link
   *     com.dynatrace.metric.util.MetricBuilderIdentifierCreator the
   *     MetricBuilderIdentifierCreator} to create the identifier from a metric builder.
   * @param newValue The new (cumulative) value which should be stored in the cache, and from which
   *                 the delta version is calculated.
   * @return The difference to the previous version, if there was already a value for the
   * identifier, or the newValue parameter otherwise.
   */
  public Long convertTotalCounterToDeltaAndUpdateCache(String identifier, Long newValue) {
    @Nullable Number oldValue = this.cache.getIfPresent(identifier);
    Long deltaValue;
    if (null == oldValue) {
      deltaValue = newValue;
    } else {
      deltaValue = newValue - oldValue.longValue();
    }
    this.cache.put(identifier, newValue);
    return deltaValue;
  }
}
