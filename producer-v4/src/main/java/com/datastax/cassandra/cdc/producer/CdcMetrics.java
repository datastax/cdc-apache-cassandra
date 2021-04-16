package com.datastax.cassandra.cdc.producer;

import com.codahale.metrics.Counter;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class CdcMetrics {
    private static final MetricNameFactory factory = new DefaultNameFactory("CdcProducer");

    public static final Counter sentMutations = Metrics.counter(factory.createMetricName("SentMutations"));
    public static final Counter sentErrors = Metrics.counter(factory.createMetricName("SentErrors"));
}
