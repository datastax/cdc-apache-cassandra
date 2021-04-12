/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.datastax.oss.kafka.source;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class Version {

    private static final String VERSION_FILE = "/cassandra-source-version.properties";

    private static String version = "undefined";

    static {
        try {
            Properties props = new Properties();
            try (InputStream inputStream = Version.class.getResourceAsStream(VERSION_FILE)) {
                props.load(inputStream);
                version = props.getProperty("version", version).trim();
            }
        } catch (IOException e) {
            log.warn("Error while loading version:", e);
        }
    }

    public static String getVersion() {
        return version;
    }
}
