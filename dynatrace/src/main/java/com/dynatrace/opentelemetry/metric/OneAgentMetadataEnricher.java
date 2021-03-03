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
import java.io.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

final class OneAgentMetadataEnricher {
  private final Logger logger;

  public OneAgentMetadataEnricher(Logger logger) {
    this.logger = logger;
  }

  Logger getLogger() {
    return this.logger;
  }

  public Collection<AbstractMap.SimpleEntry<String, String>> getDimensionsFromOneAgentMetadata() {
    String indirectionBaseName = "dt_metadata_e617c525669e072eebe3d0f08212e8f2";
    return parseOneAgentMetadata(getMetadataFileContentWithRedirection(indirectionBaseName));
  }

  /**
   * This function takes a list of strings from the OneAgent metadata file and transforms it into a
   * list of {@link AbstractMap.SimpleEntry SimpleEntry} objects. Parsing failures will not be added
   * to the output list, therefore, it is possible that the output list is shorter than the input,
   * or even empty.
   *
   * @param lines a {@link Collection<String>} containing key-value pairs (as one string) separated
   *     by an equal sign.
   * @return A {@link Collection<AbstractMap.SimpleEntry<String,String>>}. These represent the the
   *     lines passed in separated by the first occurring equal sign on each line, respectively. If
   *     no line is parsable, returns an empty list
   */
  @SuppressWarnings("JavaDoc")
  protected Collection<AbstractMap.SimpleEntry<String, String>> parseOneAgentMetadata(
      Collection<String> lines) {
    ArrayList<AbstractMap.SimpleEntry<String, String>> entries = new ArrayList<>();

    // iterate all lines from OneAgent metadata file.
    for (String line : lines) {
      getLogger().info(String.format("parsing OneAgent metadata: %s", line));
      // if there are more than one '=' in the line, split only at the first one.
      String[] split = line.split("=", 2);

      // occurs if there is no '=' in the line
      if (split.length != 2) {
        getLogger().warning(String.format("could not parse OneAgent metadata line ('%s')", line));
        continue;
      }

      String key = split[0];
      String value = split[1];

      // make sure key and value are set to non-null, non-empty values
      if ((key == null || key.isEmpty()) || (value == null || value.isEmpty())) {
        getLogger().warning(String.format("could not parse OneAgent metadata line ('%s')", line));
        continue;
      }
      entries.add(new AbstractMap.SimpleEntry<>(key, value));
    }
    return entries;
  }

  /**
   * Get the file name of the file to which OneAgent persists its parameters.
   *
   * @param fileContents A {@link Reader} object pointing at the indirection file. Will be read
   *     using a {@link BufferedReader}
   * @param indirectionBaseName The prefix for the OneAgent metadata file. Lines in the indirection
   *     file without this prefix are ignored. If null is passed as this parameter,
   *     'dt_metadata_e617c525669e072eebe3d0f08212e8f2' is used.
   * @return The string containing the filename, with no leading or trailing whitespace.
   * @throws IOException if an error occurs during reading of the file.
   */
  String getIndirectionFilename(Reader fileContents, String indirectionBaseName)
      throws IOException {
    if (fileContents == null) {
      throw new IOException("passed Reader cannot be null.");
    }
    String prefix = indirectionBaseName;
    if (indirectionBaseName == null) {
      prefix = "dt_metadata_e617c525669e072eebe3d0f08212e8f2";
    }

    String oneAgentMetadataFileName = null;
    String line;

    try (BufferedReader reader = new BufferedReader(fileContents)) {
      // read file line by line, and stop if the end of the file is reached.
      while ((line = reader.readLine()) != null) {
        line = line.trim();
        // the secret file contains the basename and a random number at the end.
        if (line.startsWith(prefix)) {
          oneAgentMetadataFileName = line;
          // if the secret file name has been found the loop can be left.
          break;
        }
      }
    }
    return oneAgentMetadataFileName;
  }

  /**
   * Read the actual content of the OneAgent metadata file.
   *
   * @param fileContents A {@link Reader} object pointing at the metadata file.
   * @return A {@link List<String>} containing the {@link String#trim() trimmed} lines.
   * @throws IOException if an error occurs during reading of the file.
   */
  List<String> getOneAgentMetadataFileContent(Reader fileContents) throws IOException {
    if (fileContents == null) {
      throw new IOException("passed Reader cannot be null.");
    }
    try (BufferedReader reader = new BufferedReader(fileContents)) {
      return reader.lines().map(String::trim).collect(Collectors.toList());
    }
  }

  /**
   * Gets the file location of the OneAgent metadata file from the indirection file and reads the
   * contents of the OneAgent metadata file.
   *
   * @return A {@link List<String>} representing the contents of the OneAgent metadata file. Leading
   *     and trailing whitespaces are {@link String#trim() trimmed} for each of the lines.
   */
  List<String> getMetadataFileContentWithRedirection(String indirectionBaseName) {
    String oneAgentMetadataFileName = null;

    try (Reader indirectionFileReader =
        new FileReader(String.format("%s.properties", indirectionBaseName))) {
      oneAgentMetadataFileName = getIndirectionFilename(indirectionFileReader, indirectionBaseName);
    } catch (FileNotFoundException e) {
      getLogger()
          .info(
              "OneAgent indirection file not found. This is normal if OneAgent is not installed.");
    } catch (IOException e) {
      getLogger()
          .info(
              String.format(
                  "Error while trying to read contents of OneAgent indirection file: %s",
                  e.getMessage()));
    }

    if (Strings.isNullOrEmpty(oneAgentMetadataFileName)) {
      return Collections.emptyList();
    }

    List<String> properties = Collections.emptyList();
    try (Reader metadataFileReader = new FileReader(oneAgentMetadataFileName)) {
      properties = getOneAgentMetadataFileContent(metadataFileReader);
    } catch (FileNotFoundException e) {
      getLogger().warning("OneAgent indirection file pointed to non existent properties file.");
    } catch (IOException e) {
      getLogger()
          .info(
              String.format(
                  "Error while trying to read contents of OneAgent metadata file: %s",
                  e.getMessage()));
    }
    return properties;
  }
}
