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
package com.datastax.oss.sink.pulsar;

import java.util.Map;
import java.util.TreeMap;

public class ConfigUtil {

    public static Map<String, String> flatString(Map<String, ?> map) {
        Map<String, String> flat = new TreeMap<>();
        for (Map.Entry<String, ?> et : map.entrySet()) flatNodeString(et, null, flat);
        return flat;
    }

    @SuppressWarnings("unchecked")
    private static void flatNodeString(
            Map.Entry<String, ?> node, String key, Map<String, String> acc) {
        String nkey = key == null ? node.getKey() : String.join(".", key, node.getKey());
        if (node.getValue() == null) return; // acc.put(nkey, null);
        else if (node.getValue() instanceof Map) {
            for (Map.Entry<String, ?> et : ((Map<String, ?>) node.getValue()).entrySet()) {
                flatNodeString(et, nkey, acc);
            }
        } else {
            String sv =
                    node.getValue() instanceof Double
                            && ((Double) node.getValue()).intValue() == (Double) node.getValue()
                            ? String.valueOf(((Double) node.getValue()).intValue())
                            : String.valueOf(node.getValue());
            acc.put(nkey, sv);
        }
    }

    public static Map<String, Object> flat(Map<String, ?> map) {
        Map<String, Object> flat = new TreeMap<>();
        for (Map.Entry<String, ?> et : map.entrySet()) flatNode(et, null, flat);
        return flat;
    }

    @SuppressWarnings("unchecked")
    private static void flatNode(Map.Entry<String, ?> node, String key, Map<String, Object> acc) {
        String nkey = key == null ? node.getKey() : String.join(".", key, node.getKey());
        if (node.getValue() == null) return; // acc.put(nkey, null);
        else if (node.getValue() instanceof Map) {
            for (Map.Entry<String, ?> et : ((Map<String, ?>) node.getValue()).entrySet()) {
                flatNode(et, nkey, acc);
            }
        } else {
            acc.put(nkey, node.getValue());
        }
    }
}
