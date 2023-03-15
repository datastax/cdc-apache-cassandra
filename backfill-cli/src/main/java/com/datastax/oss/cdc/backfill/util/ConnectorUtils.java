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

package com.datastax.oss.cdc.backfill.util;

import com.datastax.oss.cdc.backfill.dsbulk.BackfillConfigUtil;
import com.datastax.oss.dsbulk.config.ConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;

import java.util.Arrays;
import java.util.Iterator;

public class ConnectorUtils {
    public static Config createConfig(String path, Object... additionalArgs) {
        Config baseConfig = BackfillConfigUtil.
                createApplicationConfig(null, ConnectorUtils.class.getClassLoader())
                .resolve().getConfig(path);
        if (additionalArgs != null && additionalArgs.length != 0) {
            Iterator<Object> it = Arrays.asList(additionalArgs).iterator();
            while (it.hasNext()) {
                Object key = it.next();
                Object value = it.next();
                baseConfig =
                        ConfigFactory.parseString(
                                        key + "=" + value,
                                        ConfigParseOptions.defaults()
                                                .setOriginDescription("command line argument")
                                                .setClassLoader(ConnectorUtils.class.getClassLoader()))
                                .withFallback(baseConfig);
            }
        }
        return baseConfig;
    }
}
