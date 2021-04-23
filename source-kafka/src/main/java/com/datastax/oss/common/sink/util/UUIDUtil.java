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
package com.datastax.oss.common.sink.util;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.internal.core.os.Native;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MICROS;

public class UUIDUtil {
  private static final UUID KAFKA_CONNECTOR_NAMESPACE =
      UUID.fromString("bcfd612f-15fd-4b74-af81-95b04d9e756c");

  private static final DateTimeFormatter DEFAULT_TIMESTAMP_PATTERN =
      DateTimeFormatter.ofPattern("uuuuMMdd-HHmmss-SSSSSS");

  public static UUID generateClientId(String instanceName) {
    return Uuids.nameBased(KAFKA_CONNECTOR_NAMESPACE, newExecutionId(instanceName));
  }

  private static String newExecutionId(String name) {
    return name + "_" + DEFAULT_TIMESTAMP_PATTERN.format(now());
  }

  private static ZonedDateTime now() {
    // Try a native call to gettimeofday first since it has microsecond resolution,
    // and fall back to System.currentTimeMillis() if that fails
    if (Native.isCurrentTimeMicrosAvailable()) {
      return EPOCH.plus(Native.currentTimeMicros(), MICROS).atZone(UTC);
    } else {
      return Instant.now().atZone(UTC);
    }
  }
}
