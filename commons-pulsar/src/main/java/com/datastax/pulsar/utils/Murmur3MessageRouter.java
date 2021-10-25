package com.datastax.pulsar.utils;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TopicMetadata;

public class Murmur3MessageRouter implements MessageRouter {
    public final static Murmur3MessageRouter instance = new Murmur3MessageRouter();

    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
        Long token = Long.parseLong(msg.getProperty(Constants.TOKEN));
        return (short)((token >>> 48)) + Short.MAX_VALUE + 1;
    }
}
