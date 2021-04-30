/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.common.sink;

/** Fatal configuration error. */
public class ConfigException extends RuntimeException {

  public ConfigException() {}

  public ConfigException(String string) {
    super(string);
  }

  public ConfigException(String string, Throwable thrwbl) {
    super(string, thrwbl);
  }

  public ConfigException(Throwable thrwbl) {
    super(thrwbl);
  }

  public ConfigException(String settingName, Object value, String error) {
    super("Invalid value " + value + " for configuration " + settingName + ": " + error);
  }
}
