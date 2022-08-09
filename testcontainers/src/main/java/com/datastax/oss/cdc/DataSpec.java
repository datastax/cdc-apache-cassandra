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
package com.datastax.oss.cdc;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DataSpec
{
    public final String name;
    public final Object cqlValue;
    public final Object avroValue;
    public final boolean primaryKey;
    public final boolean clusteringKey;

    DataSpec(String name, Object cqlValue)
    {
        this(name, cqlValue, cqlValue, true, true);
    }

    DataSpec(String name, Object cqlValue, Object avroValue)
    {
        this(name, cqlValue, avroValue, true, true);
    }

    DataSpec(String name, Object cqlValue, Object avroValue, boolean primaryKey, boolean clusteringKey)
    {
        this.name = name;
        this.cqlValue = cqlValue;
        this.avroValue = avroValue;
        this.primaryKey = primaryKey;
        this.clusteringKey = clusteringKey;
    }
    public Object jsonValue() {
        if (avroValue instanceof ByteBuffer) {
            // jackson encodes byte[] as Base64 string
            return ((ByteBuffer) avroValue).array();
        } else if (avroValue instanceof Float) {
            // jackson encodes both doubles and floats exactly the same, it is safe to cast to a higher precision
            return ((Float) avroValue).doubleValue();
        }

        return avroValue;
    }

    public static final List<DataSpec> dataSpecs = new Vector<>();
    public static Map<String, DataSpec> dataSpecMap;
    public static List<DataSpec> pkColumns;

    static {
        final ZoneId zone = ZoneId.systemDefault();
        final LocalDate localDate = LocalDate.of(2020, 12, 25);
        final LocalDateTime localDateTime = localDate.atTime(10, 10, 0);

        dataSpecs.add(new DataSpec("text", "a", "a", true, false));
        dataSpecs.add(new DataSpec("ascii", "aa","aa"));
        dataSpecs.add(new DataSpec("boolean", true));
        dataSpecs.add(new DataSpec("blob", ByteBuffer.wrap(new byte[]{0x00, 0x01})));
        dataSpecs.add(new DataSpec("timestamp", localDateTime.atZone(zone).toInstant(), localDateTime.atZone(zone).toInstant().toEpochMilli())); // long milliseconds since epochday
        dataSpecs.add(new DataSpec("time", localDateTime.toLocalTime(), (localDateTime.toLocalTime().toNanoOfDay() / 1000))); // long microseconds since midnight
        dataSpecs.add(new DataSpec("date", localDateTime.toLocalDate(), (int) localDateTime.toLocalDate().toEpochDay())); // int seconds since epochday
        dataSpecs.add(new DataSpec("uuid", UUID.fromString("d2177dd0-eaa2-11de-a572-001b779c76e3"), "d2177dd0-eaa2-11de-a572-001b779c76e3"));
        dataSpecs.add(new DataSpec("timeuuid", UUID.fromString("d2177dd0-eaa2-11de-a572-001b779c76e3"), "d2177dd0-eaa2-11de-a572-001b779c76e3"));
        dataSpecs.add(new DataSpec("tinyint", (byte) 0x01, (int) 0x01)); // Avro only support integer
        dataSpecs.add(new DataSpec("smallint", (short) 1, (int) 1)); // Avro only support integer
        dataSpecs.add(new DataSpec("int", 1));
        dataSpecs.add(new DataSpec("bigint", Long.MAX_VALUE)); // Jackson will decide at runtime to deserialize a number as int or long, using max long value to force it to use long.
        dataSpecs.add(new DataSpec("varint", new BigInteger("314"), new CqlLogicalTypes.CqlVarintConversion().toBytes(new BigInteger("314"), CqlLogicalTypes.varintType, CqlLogicalTypes.CQL_VARINT_LOGICAL_TYPE)));
        dataSpecs.add(new DataSpec("decimal", new BigDecimal(314.16), new BigDecimal(314.16)));
        dataSpecs.add(new DataSpec("double", 1.0D));
        dataSpecs.add(new DataSpec("float", 1.0f));
        dataSpecs.add(new DataSpec("inet", Inet4Address.getLoopbackAddress(), Inet4Address.getLoopbackAddress().getHostAddress()));
        dataSpecs.add(new DataSpec("duration", CqlDuration.newInstance(1, 2, 3), CqlDuration.newInstance(1,2,3), false, false));
        dataSpecs.add(new DataSpec( "inet4", Inet4Address.getLoopbackAddress(), Inet4Address.getLoopbackAddress().getHostAddress()));
        dataSpecs.add(new DataSpec( "inet6", Inet6Address.getLoopbackAddress(), Inet6Address.getLoopbackAddress().getHostAddress()));

        dataSpecs.add(new DataSpec( "list", ImmutableList.of("a", "b", "c"), ImmutableList.of("a", "b", "c"), false, false));
        dataSpecs.add(new DataSpec( "set", ImmutableSet.of(1,2,3), ImmutableSet.of(1,2,3), false, false));
        dataSpecs.add(new DataSpec( "map", ImmutableMap.of("a",1.0,"b", 2.0, "c", 3.0), ImmutableMap.of("a",1.0,"b", 2.0, "c", 3.0), false, false));

        // list of maps
        Map<String, Double> map1 = ImmutableMap.of("a",1.0,"b", 2.0, "c", 3.0);
        List<Map<String, Double>> listOfMaps = ImmutableList.of(map1, map1, map1);
        dataSpecs.add(new DataSpec( "listofmap", listOfMaps, listOfMaps, false, false));

        dataSpecMap = dataSpecs.stream().collect(Collectors.toMap(dt -> dt.name, Function.identity()));
        pkColumns = dataSpecs.stream().filter(dt -> dt.primaryKey).collect(Collectors.toList());
    }
}
