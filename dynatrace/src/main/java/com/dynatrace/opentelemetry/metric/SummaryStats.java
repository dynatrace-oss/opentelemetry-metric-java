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

class SummaryStats {

  private SummaryStats() {}

  static LongSummaryStat longSummaryStat(long min, long max, long sum, long count) {
    return new LongSummaryStat(min, max, sum, count);
  }

  static DoubleSummaryStat doubleSummaryStat(double min, double max, double sum, long count) {
    return new DoubleSummaryStat(min, max, sum, count);
  }

  static final class LongSummaryStat implements SummaryStat {
    private final long min;
    private final long max;
    private final long sum;
    private final long count;

    public LongSummaryStat(long min, long max, long sum, long count) {
      this.min = min;
      this.max = max;
      this.sum = sum;
      this.count = count;
    }

    @Override
    public String serialize() {
      return "min=" + min + ",max=" + max + ",sum=" + sum + ",count=" + count;
    }
  }

  static final class DoubleSummaryStat implements SummaryStat {
    private final double min;
    private final double max;
    private final double sum;
    private final long count;

    public DoubleSummaryStat(double min, double max, double sum, long count) {
      this.min = min;
      this.max = max;
      this.sum = sum;
      this.count = count;
    }

    @Override
    public String serialize() {
      return "min=" + min + ",max=" + max + ",sum=" + sum + ",count=" + count;
    }
  }
}
