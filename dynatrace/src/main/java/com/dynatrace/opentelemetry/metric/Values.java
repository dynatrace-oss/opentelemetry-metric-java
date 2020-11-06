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

import com.dynatrace.opentelemetry.metric.mint.SummaryStat;
import com.dynatrace.opentelemetry.metric.mint.Value;

class Values {

  private Values() {}

  static Value doubleGauge(SummaryStat summaryStat) {
    return new DoubleGauge(null, summaryStat);
  }

  static Value doubleGauge(double value) {
    return new DoubleGauge(value, null);
  }

  static Value longGauge(SummaryStat summaryStat) {
    return new LongGauge(null, summaryStat);
  }

  static Value longGauge(long value) {
    return new LongGauge(value, null);
  }

  static Value doubleCount(double value, boolean isDelta) {
    return new DoubleCount(value, isDelta);
  }

  static Value longCount(long value, boolean isDelta) {
    return new LongCount(value, isDelta);
  }

  abstract static class AbstractValue implements Value {

    final String qualifier;

    private AbstractValue(String qualifier) {
      this.qualifier = qualifier;
    }
  }

  static final class DoubleGauge extends AbstractValue {
    private final Double value;
    private final SummaryStat summaryStat;

    public DoubleGauge(Double value, SummaryStat summaryStat) {
      super("gauge");
      this.value = value;
      this.summaryStat = summaryStat;
    }

    @Override
    public String serialize() {
      if (summaryStat == null) {
        return qualifier + "," + value;
      }
      return qualifier + "," + summaryStat.serialize();
    }
  }

  static final class LongGauge extends AbstractValue {
    private final Long value;
    private final SummaryStat summaryStat;

    private LongGauge(Long value, SummaryStat summaryStat) {
      super("gauge");
      this.value = value;
      this.summaryStat = summaryStat;
    }

    @Override
    public String serialize() {
      if (summaryStat == null) {
        return qualifier + "," + value;
      }
      return qualifier + "," + summaryStat.serialize();
    }
  }

  static final class DoubleCount extends AbstractValue {
    private final double value;
    private final boolean isDelta;

    public DoubleCount(double value, boolean isDelta) {
      super("count");
      this.value = value;
      this.isDelta = isDelta;
    }

    @Override
    public String serialize() {
      if (isDelta) {
        return qualifier + ",delta=" + value;
      }
      return qualifier + "," + value;
    }
  }

  static final class LongCount extends AbstractValue {
    private final long value;
    private final boolean isDelta;

    public LongCount(long value, boolean isDelta) {
      super("count");
      this.value = value;
      this.isDelta = isDelta;
    }

    @Override
    public String serialize() {
      if (isDelta) {
        return qualifier + ",delta=" + value;
      }
      return qualifier + "," + value;
    }
  }
}
