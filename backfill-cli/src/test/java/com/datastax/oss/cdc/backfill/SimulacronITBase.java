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

package com.datastax.oss.cdc.backfill;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronExtension;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Column;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Keyspace;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Table;
import com.datastax.oss.dsbulk.tests.simulacron.annotations.SimulacronConfig;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.datastax.oss.simulacron.server.BoundCluster;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

@ExtendWith(SimulacronExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SimulacronConfig(dseVersion = "")
abstract class SimulacronITBase {

    final BoundCluster origin;
    final String originHost;

    SimulacronITBase(BoundCluster origin) {
        this.origin = origin;
        this.originHost =
                this.origin.node(0).inetSocketAddress().getHostString()
                        + ':'
                        + this.origin.node(0).inetSocketAddress().getPort();
    }

    @BeforeEach
    void resetSimulacron() {
        List<Column> pks = new ArrayList<>();
        // Priming tables with small letter PK causes dsbulk to return empty rows although the prime dsl and
        // the logged query in origin.getLogs(true) match.
        pks.add(new Column("textPK", DataTypes.INT));

        SimulacronUtils.primeTables(
                origin,
                new Keyspace(
                        "test",
                        new Table(
                                "t1",
                                pks,
                                Collections.emptyList(),
                                Collections.emptyList()
                        )));

        LinkedHashMap<String, String> columnTypes =
                map("textPK", "ascii");
        List<LinkedHashMap<String, Object>> rows = new ArrayList<>();
        rows.add(map("textPK", "first"));
        rows.add(map("textPK", "second"));
        origin.prime(
                PrimeDsl.when("SELECT textPK FROM test.t1 WHERE token(\"textPK\") > :start AND token(\"textPK\") <= :end")
                        .then(new SuccessResult(rows, columnTypes)));
    }

    static <K, V> LinkedHashMap<K, V> map(Object... keysAndValues) {
        LinkedHashMap<Object, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length - 1; i += 2) {
            map.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return (LinkedHashMap<K, V>) map;
    }

    private static SuccessResult createReadResult() {
        List<LinkedHashMap<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            LinkedHashMap<String, Object> row = new LinkedHashMap<>();
            row.put("pk", i);
            rows.add(row);
        }
        LinkedHashMap<String, String> columnTypes = new LinkedHashMap<>();
        columnTypes.put("pk", "int");
        return new SuccessResult(rows, columnTypes);
    }
}
