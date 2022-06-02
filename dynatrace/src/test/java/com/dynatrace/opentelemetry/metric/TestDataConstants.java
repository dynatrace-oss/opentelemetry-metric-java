package com.dynatrace.opentelemetry.metric;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.resources.Resource;
import org.assertj.core.data.Offset;

public class TestDataConstants {
  private TestDataConstants() {}

  public static final String DEFAULT_NAME = "defaultInstrumentName";
  public static final String DEFAULT_UNIT = "defaultUnit";
  public static final String DEFAULT_DESC = "default description to be used in tests";

  // These timestamps are 3 seconds apart to simulate data being recorded in an order.
  // 01/01/2022 00:00:00.
  public static final long MILLIS_TS_1 = 1_640_991_600_000L;
  public static final long MILLIS_TS_2 = MILLIS_TS_1 + 3000;
  public static final long MILLIS_TS_3 = MILLIS_TS_2 + 3000;
  public static final long MILLIS_TS_4 = MILLIS_TS_3 + 3000;

  public static final long NANOS_TS_1 = MILLIS_TS_1 * 1_000_000;
  public static final long NANOS_TS_2 = MILLIS_TS_2 * 1_000_000;
  public static final long NANOS_TS_3 = MILLIS_TS_3 * 1_000_000;
  public static final long NANOS_TS_4 = MILLIS_TS_4 * 1_000_000;

  public static Offset<Double> OFFSET = Offset.offset(0.0001d);

  public static final Attributes EMPTY_ATTRIBUTES = Attributes.empty();
  public static final InstrumentationScopeInfo DEFAULT_SCOPE =
      InstrumentationScopeInfo.create("testing_scope");
  public static final Resource DEFAULT_RESOURCE = Resource.getDefault();
}
