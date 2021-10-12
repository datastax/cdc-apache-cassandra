package com.datastax.cassandra.cdc;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Assert;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

public class ProducerTestUtil {

    private static final Random random = new Random();

    public static final DockerImageName PULSAR_IMAGE = DockerImageName.parse(
            Optional.ofNullable(System.getenv("PULSAR_IMAGE"))
                    .orElse("datastax/lunastreaming:2.7.2_1.1.6")
    ).asCompatibleSubstituteFor("pulsar");

    public static String genericRecordToString(GenericRecord genericRecord) {
        StringBuilder sb = new StringBuilder("{");
        for (Field field : genericRecord.getFields()) {
            if (sb.length() > 1)
                sb.append(",");
            sb.append(field.getName()).append("=");
            if (genericRecord.getField(field) instanceof GenericRecord) {
                sb.append(genericRecordToString((GenericRecord) genericRecord.getField(field)));
            } else {
                sb.append(genericRecord.getField(field) == null ? "null" : genericRecord.getField(field).toString());
            }
        }
        return sb.append("}").toString();
    }

    public static Map<String, Object> genericRecordToMap(GenericRecord genericRecord) {
        Map<String, Object> map = new HashMap<>();
        for (Field field : genericRecord.getFields()) {
            map.put(field.getName(), genericRecord.getField(field));
        }
        return map;
    }

    public static void assertGenericRecords(String field, GenericRecord gr, Map<String, Object[]> values) {
        switch (field) {
            case "decimal": {
                ByteBuffer bb = (ByteBuffer) gr.getField(CqlLogicalTypes.CQL_DECIMAL_BIGINT);
                byte[] bytes = new byte[bb.remaining()];
                bb.duplicate().get(bytes);
                BigInteger bigInteger = new BigInteger(bytes);
                BigDecimal bigDecimal = new BigDecimal(bigInteger, (int) gr.getField(CqlLogicalTypes.CQL_DECIMAL_SCALE));
                Assert.assertEquals("Wrong value for field " + field, values.get(field)[1], bigDecimal);
            }
            break;
            case "duration": {
                Assert.assertEquals("Wrong value for field " + field, values.get(field)[1],
                        CqlDuration.newInstance(
                                (int) gr.getField(CqlLogicalTypes.CQL_DURATION_MONTHS),
                                (int) gr.getField(CqlLogicalTypes.CQL_DURATION_DAYS),
                                (long) gr.getField(CqlLogicalTypes.CQL_DURATION_NANOSECONDS)));
            }
        }
    }

    public static ByteBuffer randomizeBuffer(int size)
    {
        byte[] toWrap = new byte[size];
        random.nextBytes(toWrap);
        return ByteBuffer.wrap(toWrap);
    }

    public enum Version {
        V3,
        V4,
        DSE
    }
}
