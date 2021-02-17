# Quasar consumer

Quasar consumers are a replacement of Apache Pulsar consumers. 
For a given primary key, it replicates Cassandra mutations to Elasticsearch 
sequentially to avoid a possible race condition.

## Docker build

    ./gradlew quasar-consumer:jib
    

## Configuration

See the [application.yml](src/main/resources/application.yml) file.

## Run

    SERVER_HOST=127.0.0.1 QUASAR_NODE_NAME=quasar-0 ./gradlew quasar-consumer:run
    SERVER_HOST=127.0.0.2 QUASAR_NODE_NAME=quasar-1 ./gradlew quasar-consumer:run
    SERVER_HOST=127.0.0.3 QUASAR_NODE_NAME=quasar-2 ./gradlew quasar-consumer:run

The health endpoint test the connectivity to both Cassandra and Elasticsearch:

    http://localhost:8085/health

Readiness endpoint:

    http://localhost:8081/ready

## Monitoring

List of available metrics:

    http://localhost:8085/metrics

Prometheus metrics:

    http://localhost:8085/prometheus

## Swagger documentation

    http://localhost:8081/swagger/quasar-1.0.yml
    http://localhost:8081/swagger/views/swagger-ui/