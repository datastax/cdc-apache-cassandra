/**
 * Copyright DataStax, Inc 2021.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.common.sink.config;

import com.datastax.oss.common.sink.ConfigException;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.kafka.common.config.AbstractConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Collectors;

/** Helper methods useful for performing common tasks in *Config classes. */
public class ConfigUtil {

  /** This is a utility class; no one should instantiate it. */
  private ConfigUtil() {}

  /**
   * Produce a formatted string of the given settings in the given config object.
   *
   * @param config object containing settings to extract.
   * @param prefixToExcise setting name prefix to remove when emitting setting names (e.g. "auth."
   *     or "ssl.").
   * @param settingNames names of settings to extract.
   * @return lines of the form "setting: value".
   */
  public static String configToString(
      AbstractConfig config, String prefixToExcise, String... settingNames) {
    return Arrays.stream(settingNames)
        .map(
            s ->
                String.format(
                    "%s: %s",
                    s.substring(prefixToExcise.length()), config.values().get(s).toString()))
        .collect(Collectors.joining("\n"));
  }

  /**
   * Convert the given setting value to an absolute path.
   *
   * @param settingValue setting to convert
   * @return the converted path.
   */
  public static @Nullable Path getFilePath(@Nullable String settingValue) {
    return settingValue == null || settingValue.isEmpty()
        ? null
        : Paths.get(settingValue).toAbsolutePath().normalize();
  }

  /**
   * Verify that the given file-path exists, is a simple file, and is readable.
   *
   * @param filePath file path to validate
   * @param settingName name of setting whose value is filePath; used in generating error messages
   *     for failures.
   */
  public static void assertAccessibleFile(@Nullable Path filePath, String settingName) {
    if (filePath == null) {
      // There's no path to check.
      return;
    }

    if (!Files.exists(filePath)) {
      throw new ConfigException(settingName, filePath.toString(), "does not exist");
    }
    if (!Files.isRegularFile(filePath)) {
      throw new ConfigException(settingName, filePath.toString(), "is not a file");
    }
    if (!Files.isReadable(filePath)) {
      throw new ConfigException(settingName, filePath.toString(), "is not readable");
    }
  }
}
