package com.datastax.oss.pulsar.source;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class ConverterAndQuery {
    final Converter converter;
    final String query;
}
