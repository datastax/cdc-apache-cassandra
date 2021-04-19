# Kafka notes

## Converters

    see https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/#configuring-converters

## Consumer group

    /usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --list
    /usr/bin/kafka-consumer-groups --bootstrap-server localhost:9092 --group <your group name> --describe

## Topic

    kafkacat -L -b localhost:55147 -t events-ks1.table1 -J
    kafkacat -C -b localhost:55147 -t events-ks1.table1 -J
