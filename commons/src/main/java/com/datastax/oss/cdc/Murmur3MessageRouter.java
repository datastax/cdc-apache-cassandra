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

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TopicMetadata;

public class Murmur3MessageRouter implements MessageRouter {
    public final static Murmur3MessageRouter instance = new Murmur3MessageRouter();

    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
        Long token = Long.parseLong(msg.getProperty(Constants.TOKEN));
        return ((short)((token >>> 48)) + Short.MAX_VALUE + 1 ) % metadata.numPartitions();
    }
}
