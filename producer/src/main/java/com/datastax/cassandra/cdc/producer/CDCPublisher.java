package com.datastax.cassandra.cdc.producer;

import com.datastax.cassandra.cdc.CDCSchema;
import com.datastax.cassandra.cdc.PrimaryKey;
import com.datastax.cassandra.cdc.PulsarConfiguration;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.schema.KeyValue;

import javax.inject.Singleton;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@Singleton
public class CDCPublisher {

    PulsarConfiguration pulsarConfiguration;

    public CDCPublisher(PulsarConfiguration pulsarConfiguration) {
        this.pulsarConfiguration = pulsarConfiguration;
    }

    public void publish() {
        PulsarClient client = null;
        Producer<KeyValue<PrimaryKey, Timestamp>> producer = null;
        try {
            client = PulsarClient.builder()
                    .serviceUrl(pulsarConfiguration.getServiceUrl())
                    .build();

            producer = client.newProducer(CDCSchema.kvSchema)
                    .producerName("producer-1")
                    .topic(pulsarConfiguration.getTopic())
                    .sendTimeout(0, TimeUnit.SECONDS)
                    .create();

            Timestamp writetime = new Timestamp(new Date().getTime());
            KeyValue<PrimaryKey, Timestamp> kv = new KeyValue<>(
                    new PrimaryKey("ks1","table1", new Object[] { "1" }),
                    writetime);
            producer.send(kv);
            System.out.println("Message sent");
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
            if(producer != null) {
                try {
                    producer.close();
                } catch(Exception e) {
                }
            }
            if(client != null) {
                try {
                    client.close();
                } catch(Exception e) {
                }
            }
        }
    }
}
