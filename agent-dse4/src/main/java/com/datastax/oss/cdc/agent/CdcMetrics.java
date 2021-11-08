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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import org.apache.cassandra.metrics.DefaultNameFactory;
import org.apache.cassandra.metrics.MetricNameFactory;

import static org.apache.cassandra.metrics.CassandraMetricsRegistry.Metrics;

public class CdcMetrics {
    public static final String CDC_AGENT_MBEAN_NAME = "CdcAgent";
    private static final MetricNameFactory factory = new DefaultNameFactory(CDC_AGENT_MBEAN_NAME);

    public static final Counter sentMutations = Metrics.counter(factory.createMetricName("SentMutations"));
    public static final Counter sentErrors = Metrics.counter(factory.createMetricName("SentErrors"));

    public static final Counter commitLogReadErrors = Metrics.counter(factory.createMetricName("CommitLogReadErrors"));
    public static final Counter skippedMutations = Metrics.counter(factory.createMetricName("SkippedMutations"));

    public static final Counter executedTasks = Metrics.counter(factory.createMetricName("ExecutedTasks"));

    public static final Gauge<Integer> submittedTasksGauge = Metrics.register(factory.createMetricName("SubmittedTasks"), new Gauge<Integer>()
    {
        public Integer getValue()
        {
            return CommitLogReaderService.submittedTasks.size();
        }
    });

    public static final Gauge<Integer> maxSubmittedTasks = Metrics.register(factory.createMetricName("MaxSubmittedTasks"), new Gauge<Integer>()
    {
        public Integer getValue()
        {
            return CommitLogReaderService.maxSubmittedTasks.get();
        }
    });

    public static final Gauge<Integer> pendingTasksGauge = Metrics.register(factory.createMetricName("PendingTasks"), new Gauge<Integer>()
    {
        public Integer getValue()
        {
            return CommitLogReaderService.pendingTasks.size();
        }
    });

    public static final Gauge<Integer> maxPendingTasks = Metrics.register(factory.createMetricName("PaxPendingTasks"), new Gauge<Integer>()
    {
        public Integer getValue()
        {
            return CommitLogReaderService.maxPendingTasks.get();
        }
    });

    public static final Gauge<Integer> uncleanedTasksGauge = Metrics.register(factory.createMetricName("UncleanedTasks"), new Gauge<Integer>()
    {
        public Integer getValue()
        {
            return CommitLogReaderService.pendingTasks.size();
        }
    });

    public static final Gauge<Integer> maxUncleanedTasks = Metrics.register(factory.createMetricName("MaxUncleanedTasks"), new Gauge<Integer>()
    {
        public Integer getValue()
        {
            return CommitLogReaderService.maxUncleanedTasks.get();
        }
    });
}
