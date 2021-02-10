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
package com.dynatrace.opentelemetry.metric.mint;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@AutoValue
public abstract class Datapoint implements MintLineProtocolSerializable {

  public abstract List<Dimension> dimensions();

  public abstract long timestamp();

  public abstract String key();

  public abstract Value value();

  @Override
  public String serialize() {
    final StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(key());
    List<Dimension> dimensions = dimensions();
    if (!dimensions.isEmpty()) {
      stringBuilder.append(",");
      for (int i = 0; i < dimensions.size(); i++) {
        if (i > 0) {
          stringBuilder.append(",");
        }
        stringBuilder.append(dimensions.get(i).serialize());
      }
    }
    stringBuilder.append(" ").append(value().serialize());
    stringBuilder.append(" ").append(TimeUnit.NANOSECONDS.toMillis(timestamp()));
    return stringBuilder.toString();
  }

  public static Builder create(String key) {
    return new AutoValue_Datapoint.Builder().key(key).dimensions(new ArrayList<Dimension>());
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder key(String key);

    public abstract Builder dimensions(List<Dimension> dimensions);

    protected abstract List<Dimension> dimensions();

    /**
     * Add a single dimension to the list of dimensions.
     *
     * @param key of the dimension.
     * @param value of the dimension.
     * @return this.
     */
    public Builder addDimension(String key, String value) {
      dimensions().add(Dimension.create(key, value));
      return this;
    }

    public abstract Builder timestamp(long timestamp);

    public abstract Builder value(Value value);

    public Datapoint build() {
      return dimensions(Collections.unmodifiableList(dimensions())).autoBuild();
    }

    abstract Datapoint autoBuild();
  }
}
