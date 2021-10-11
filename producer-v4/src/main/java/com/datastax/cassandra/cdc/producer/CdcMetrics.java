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
package com.datastax.cassandra.cdc.producer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class CdcMetrics {
    private static final MetricNameFactory factory = new DefaultNameFactory("CdcProducer");

    public static final Counter sentMutations = Metrics.counter(factory.createMetricName("SentMutations"));
    public static final Counter sentErrors = Metrics.counter(factory.createMetricName("SentErrors"));
    public static final Counter commitLogReadErrors = Metrics.counter(factory.createMetricName("CommitLogReadErrors"));
    public static final Counter skippedMutations = Metrics.counter(factory.createMetricName("SkippedMutations"));

    public static final Gauge<Integer> runningTasksGauge = Metrics.register(factory.createMetricName("runningTasks"), new Gauge<Integer>()
    {
        public Integer getValue()
        {
            return CommitLogReaderService.runningTasks.size();
        }
    });

    public static final Gauge<Integer> maxRunningTasksGauge = Metrics.register(factory.createMetricName("maxRunningTasks"), new Gauge<Integer>()
    {
        public Integer getValue()
        {
            return CommitLogReaderService.maxRunningTasksGauge;
        }
    });

    public static final Gauge<Integer> pendingTasksGauge = Metrics.register(factory.createMetricName("pendingTasks"), new Gauge<Integer>()
    {
        public Integer getValue()
        {
            return CommitLogReaderService.pendingTasks.size();
        }
    });

    public static final Gauge<Integer> maxPendingTasksGauge = Metrics.register(factory.createMetricName("maxPendingTasks"), new Gauge<Integer>()
    {
        public Integer getValue()
        {
            return CommitLogReaderService.maxPendingTasksGauge;
        }
    });
}
