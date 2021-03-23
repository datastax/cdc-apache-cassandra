package com.datastax.oss.pulsar.source;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import edu.umd.cs.findbugs.annotations.NonNull;
import lombok.EqualsAndHashCode;
import lombok.ToString;


@EqualsAndHashCode
@ToString
public class ColumnMetadataImpl implements ColumnMetadata {
    CqlIdentifier keyspace;
    CqlIdentifier parent;
    CqlIdentifier name;
    DataType type;
    boolean  isStatic;

    public ColumnMetadataImpl(
            CqlIdentifier keyspace,
            CqlIdentifier parent,
            CqlIdentifier name,
            DataType type,
            boolean isStatic
    ) {
        this.keyspace = keyspace;
        this.parent = parent;
        this.name = name;
        this.type = type;
        this.isStatic = isStatic;
    }

    @NonNull
    @Override
    public CqlIdentifier getKeyspace() {
        return keyspace;
    }

    @NonNull
    @Override
    public CqlIdentifier getParent() {
        return parent;
    }

    @NonNull
    @Override
    public CqlIdentifier getName() {
        return name;
    }

    @NonNull
    @Override
    public DataType getType() {
        return type;
    }

    @Override
    public boolean isStatic() {
        return isStatic;
    }
}
