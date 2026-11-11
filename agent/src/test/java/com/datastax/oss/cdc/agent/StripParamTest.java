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
package com.datastax.oss.cdc.agent;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class StripParamTest {

    /** The regex implementation stripParam used to have, kept here as the reference. */
    private static String regexStrip(String agentArgs, String paramName) {
        return agentArgs
                .replaceAll("(?:^|(?<=,))\\s*" + paramName + "=[^,]*(?:,|$)", "")
                .replaceAll(",+$", "")
                .replaceAll("^,+", "")
                .replaceAll(",{2,}", ",")
                .trim();
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "platform=kafka",
            "platform=kafka,topicPrefix=events-",
            "topicPrefix=events-,platform=kafka",
            "a=1,platform=kafka,b=2",
            "a=1, platform=kafka, b=2",
            "a=1 , platform=kafka,b=2",
            "xplatform=1,platform=pulsar",
            "a=1,,b=2",
            ",a=1,",
            " a=1 ",
            "a=1",
            "",
            "platform=",
            "a=platform=x,b=2"
    })
    void matchesTheOldRegex(String args) {
        assertEquals(regexStrip(args, "platform"), AgentConfig.stripParam(args, "platform"));
    }

    @Test
    void handlesNull() {
        assertNull(AgentConfig.stripParam(null, "platform"));
    }
}
