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
import com.datastax.oss.driver.shaded.guava.common.net.InetAddresses;
import com.datastax.oss.driver.shaded.guava.common.net.InternetDomainName;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ContactPointsValidator {

    public static final String CONTACT_POINTS_OPT = "contactPoints";

  /**
   * This method validates all contact points and throws ConfigException if there is at least one
   * invalid. A valid contact point is either a valid IP address in its canonical form, or a valid
   * domain name.
   *
   * @param contactPoints - list of contact points to validate
   */
  public static void validateContactPoints(List<String> contactPoints) {
    Set<String> invalid =
        contactPoints
            .stream()
            .filter(ContactPointsValidator::isInvalidAddress)
            .collect(Collectors.toSet());
    if (!invalid.isEmpty()) {
      throw new ConfigException(
          String.format(
              "Incorrect %s: %s",
              ContactPointsValidator.CONTACT_POINTS_OPT, String.join(",", invalid)));
    }
  }

  public static boolean isInvalidAddress(String contactPoint) {
    return !InetAddresses.isInetAddress(contactPoint) && !InternetDomainName.isValid(contactPoint);
  }
}
