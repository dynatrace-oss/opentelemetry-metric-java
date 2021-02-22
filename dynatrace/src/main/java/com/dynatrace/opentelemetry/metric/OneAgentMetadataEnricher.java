/*
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

import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OneAgentMetadataEnricher {
  private final Logger logger;

  public OneAgentMetadataEnricher(Logger logger) {
    this.logger = logger;
  }

  public Collection<AbstractMap.SimpleEntry<String, String>> getDimensionsFromOneAgentMetadata() {
    return parseOneAgentMetadata(getMetadataFileContent());
  }

  /**
   * This function takes a list of strings from the OneAgent metadata file and transforms it into a
   * list of {@link AbstractMap.SimpleEntry SimpleEntry} objects. Parsing failures will not be added
   * to the output list, therefore, it is possible that the output list is shorter than the input,
   * or even empty.
   *
   * @param lines a {@link Collection<String>} containing key-value pairs (as one string) separated
   *     by an equal sign.
   * @return A {@link Collection<AbstractMap.SimpleEntry<String, String>>}. These represent the the
   *     lines passed in separated by the first occurring equal sign on each line, respectively. If
   *     no line is parsable, returns an empty list
   */
  protected Collection<AbstractMap.SimpleEntry<String, String>> parseOneAgentMetadata(
      Collection<String> lines) {
    ArrayList<AbstractMap.SimpleEntry<String, String>> entries = new ArrayList<>();

    // iterate all lines from OneAgent metadata file.
    for (String line : lines) {
      logger.info(String.format("parsing OneAgent metadata: %s", line));
      // if there are more than one '=' in the line, split only at the first one.
      String[] split = line.split("=", 2);

      // occurs if there is no '=' in the line
      if (split.length != 2) {
        logger.warning(String.format("could not parse OneAgent metadata line ('%s')", line));
        continue;
      }

      String key = split[0];
      String value = split[1];

      // make sure key and value are set to non-null, non-empty values
      if ((key == null || key.isEmpty()) || (value == null || value.isEmpty())) {
        logger.warning(String.format("could not parse OneAgent metadata line ('%s')", line));
        continue;
      }
      entries.add(new AbstractMap.SimpleEntry<>(key, value));
    }
    return entries;
  }

  private List<String> getMetadataFileContent() {
    String indirectionBaseName = "dt_metadata_e617c525669e072eebe3d0f08212e8f2";
    String secretFileName = null;
    try (BufferedReader reader =
        new BufferedReader(new FileReader(String.format("%s.properties", indirectionBaseName)))) {
      String line = "";
      // this whole while block is to make sure this function will still work even if the contents
      // of the
      // indirection file were to change in the future.

      // read file line by line, and stop if the end of the file is reached.
      while ((line = reader.readLine()) != null) {
        // the secret file contains the basename and a random number at the end.
        if (line.contains(indirectionBaseName)) {
          secretFileName = line;
          // if the secret file name has been found the loop can be left.
          break;
        }
      }
    } catch (FileNotFoundException ignore) {
      logger.info("OneAgent metadata file not found. This is normal if OneAgent is not installed.");
    } catch (IOException e) {
      logger.info(
          String.format(
              "Error while trying to read contents of OneAgent metadata file: %s", e.getMessage()));
    }

    if (!Strings.isNullOrEmpty(secretFileName)) {
      List<String> properties;
      // read all lines in the secret file into the properties list:
      try (Stream<String> lines = Files.lines(Paths.get(secretFileName))) {
        properties = lines.collect(Collectors.toList());
      } catch (IOException e) {
        // in this case we can probably raise a warning, as the magic file has been found, but not
        // the
        // properties file pointed to by the magic file.
        logger.warning(
            String.format("OneAgent properties file could not be read: %s", e.getMessage()));
        properties = Collections.emptyList();
      }
      return properties;
    }

    return Collections.emptyList();
  }
}
