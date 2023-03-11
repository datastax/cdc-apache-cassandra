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

package com.datastax.oss.cdc.backfill.dsbulk;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import java.nio.file.Path;

/**
 * Class loader aware version of {@link com.datastax.oss.dsbulk.config.ConfigUtils}.
 */
public class BackfillConfigUtil {

    @NonNull
    public static Config createReferenceConfig(ClassLoader classLoader) {
        return ConfigFactory.parseResourcesAnySyntax("dsbulk-reference",
                ConfigParseOptions.defaults().setClassLoader(classLoader))
                .withFallback(ConfigFactory.parseResourcesAnySyntax("driver-reference",
                        ConfigParseOptions.defaults().setClassLoader(classLoader)))
                .withFallback(ConfigFactory.defaultReferenceUnresolved());
    }

    @NonNull
    public static Config createApplicationConfig(@Nullable Path appConfigPath, ClassLoader classLoader) {
        try {
            if (appConfigPath != null) {
                System.setProperty("config.file", appConfigPath.toString());
            }

            Config referenceConfig = createReferenceConfig(classLoader);
            return ConfigFactory.defaultOverrides().withFallback(ConfigFactory.defaultApplication()).withFallback(referenceConfig);
        } catch (ConfigException.Parse var2) {
            throw new IllegalArgumentException(String.format("Error parsing configuration file %s at line %s. Please make sure its format is compliant with HOCON syntax. If you are using \\ (backslash) to define a path, escape it with \\\\ or use / (forward slash) instead.", var2.origin().filename(), var2.origin().lineNumber()), var2);
        }
    }
}
