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
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static com.datastax.oss.common.sink.config.ConfigUtil.*;

/** Authenticator configuration */
public class AuthenticatorConfig extends AbstractConfig {
  public static final String PROVIDER_OPT = "auth.provider";
  public static final String USERNAME_OPT = "auth.username";
  public static final String PASSWORD_OPT = "auth.password";
  public static final String KEYTAB_OPT = "auth.gssapi.keyTab";
  public static final String PRINCIPAL_OPT = "auth.gssapi.principal";
  public static final String SERVICE_OPT = "auth.gssapi.service";

  private static final Logger log = LoggerFactory.getLogger(AuthenticatorConfig.class);
  private static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              PROVIDER_OPT,
              ConfigDef.Type.STRING,
              "None",
              ConfigDef.Importance.HIGH,
              "None | PLAIN | GSSAPI")
          .define(
              USERNAME_OPT,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.HIGH,
              "Username for PLAIN (username/password) provider authentication")
          .define(
              PASSWORD_OPT,
              ConfigDef.Type.PASSWORD,
              "",
              ConfigDef.Importance.HIGH,
              "Password for PLAIN (username/password) provider authentication")
          .define(
              KEYTAB_OPT,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.HIGH,
              "Kerberos keytab file for GSSAPI provider authentication")
          .define(
              PRINCIPAL_OPT,
              ConfigDef.Type.STRING,
              "",
              ConfigDef.Importance.HIGH,
              "Kerberos principal for GSSAPI provider authentication")
          .define(
              SERVICE_OPT,
              ConfigDef.Type.STRING,
              "dse",
              ConfigDef.Importance.HIGH,
              "SASL service name to use for GSSAPI provider authentication");

  @Nullable private final Path keyTabPath;

  public AuthenticatorConfig(Map<String, String> authSettings) {
    super(CONFIG_DEF, sanitizeAuthSettings(authSettings), false);

    // Verify that the provider value is valid.
    Provider provider = getProvider();

    // If password is specified, username must be.
    if (provider == Provider.PLAIN && !getPassword().isEmpty() && getUsername().isEmpty()) {
      throw new ConfigException(
          String.format("%s was specified without %s", PASSWORD_OPT, USERNAME_OPT));
    }

    keyTabPath = getFilePath(getString(KEYTAB_OPT));
    if (provider == Provider.GSSAPI) {
      if (getService().isEmpty()) {
        throw new ConfigException(SERVICE_OPT, "<empty>", "is required");
      }

      assertAccessibleFile(keyTabPath, KEYTAB_OPT);
    }
  }

  /**
   * Auth settings may not be fully specified. Fill in the gaps as appropriate:
   *
   * <ol>
   *   <li>If username or password is provided, and auth-provider is None, coerce it to be the PLAIN
   *       auth provider.
   *   <li>If using GSSAPI and the principal isn't provided, try to find it from the keytab (if
   *       provided).
   * </ol>
   *
   * @param authSettings map of authentication settings
   * @return fully specified auth settings
   */
  private static Map<String, String> sanitizeAuthSettings(Map<String, String> authSettings) {
    Map<String, String> mutated = new HashMap<>(authSettings);
    String provider = authSettings.get(PROVIDER_OPT);
    if ((authSettings.containsKey(USERNAME_OPT) || authSettings.containsKey(PASSWORD_OPT))
        && ("None".equals(provider) || provider == null)) {
      // Username/password was provided. Coerce the provider type to PLAIN.
      mutated.put(PROVIDER_OPT, "PLAIN");
    }

    // If the provider is GSSAPI and the principal isn't specified,
    // try to get the first principal from the keytab file (if present).
    if ("GSSAPI".equals(provider)
        && !authSettings.containsKey(PRINCIPAL_OPT)
        && authSettings.containsKey(KEYTAB_OPT)) {
      Path keyTabPath = getFilePath(authSettings.get(KEYTAB_OPT));
      if (keyTabPath != null) {
        assertAccessibleFile(keyTabPath, KEYTAB_OPT);
        mutated.put(PRINCIPAL_OPT, getPrincipalFromKeyTab(keyTabPath.toString()));
      }
    }
    return ImmutableMap.<String, String>builder().putAll(mutated).build();
  }

  private static String getPrincipalFromKeyTab(String keyTabFile) {
    // Best effort: get the first principal in the keytab, if possible.
    // We use reflection because we're referring to sun internal kerberos classes:
    // sun.security.krb5.internal.ktab.KeyTab;
    // sun.security.krb5.internal.ktab.KeyTabEntry;
    // The code below is equivalent to the following:
    //
    // keyTab = KeyTab.getInstance(keyTabFile);
    // KeyTabEntry[] entries = keyTab.getEntries();
    // if (entries.length > 0) {
    //   principal = entries[0].getService().getName();
    //   LOGGER.debug("Found Kerberos principal %s in %s", principal, keyTabFile);
    // } else {
    //   throw new ConfigException(
    //     String.format("Cannot start Studio, cannot find any principals in %s", keyTabFile));
    // }
    String principal;
    try {
      Class<?> keyTabClazz = Class.forName("sun.security.krb5.internal.ktab.KeyTab");
      Class<?> keyTabEntryClazz = Class.forName("sun.security.krb5.internal.ktab.KeyTabEntry");
      Class<?> principalNameClazz = Class.forName("sun.security.krb5.PrincipalName");
      Method getInstanceMethod = keyTabClazz.getMethod("getInstance", String.class);
      Method getEntriesMethod = keyTabClazz.getMethod("getEntries");
      Method getServiceMethod = keyTabEntryClazz.getMethod("getService");
      Method getNameMethod = principalNameClazz.getMethod("getName");
      Object keyTab = getInstanceMethod.invoke(null, keyTabFile);
      Object[] entries = (Object[]) getEntriesMethod.invoke(keyTab);
      if (entries.length > 0) {
        principal = (String) getNameMethod.invoke(getServiceMethod.invoke(entries[0]));
        log.debug("Found Kerberos principal {} in {}", principal, keyTabFile);
        return principal;
      } else {
        throw new ConfigException(String.format("Cannot find any principals in %s", keyTabFile));
      }
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | IllegalAccessException
        | InvocationTargetException e) {
      throw new ConfigException(
          String.format("Cannot find any principals in %s: %s", keyTabFile, e.getMessage()));
    }
  }

  public Provider getProvider() {
    String providerString = getString(PROVIDER_OPT);
    try {
      return Provider.valueOf(providerString);
    } catch (IllegalArgumentException e) {
      throw new ConfigException(
          PROVIDER_OPT, providerString, "valid values are None, PLAIN, GSSAPI");
    }
  }

  public String getPassword() {
    return getPassword(PASSWORD_OPT).value();
  }

  public String getUsername() {
    return getString(USERNAME_OPT);
  }

  @Nullable
  public Path getKeyTabPath() {
    return keyTabPath;
  }

  public String getPrincipal() {
    return getString(PRINCIPAL_OPT);
  }

  public String getService() {
    return getString(SERVICE_OPT);
  }

  @Override
  public String toString() {
    return configToString(
        this,
        "auth.",
        PROVIDER_OPT,
        USERNAME_OPT,
        PASSWORD_OPT,
        KEYTAB_OPT,
        PRINCIPAL_OPT,
        SERVICE_OPT);
  }

  public enum Provider {
    None,
    PLAIN,
    GSSAPI
  }
}
