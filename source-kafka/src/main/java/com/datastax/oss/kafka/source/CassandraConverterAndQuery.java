package com.datastax.oss.kafka.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class CassandraConverterAndQuery {
    final CassandraConverter converter;
    final String query;
}
