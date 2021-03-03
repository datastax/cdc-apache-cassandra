package org.apache.cassandra.schema;

import org.apache.cassandra.db.Mutation;

import java.util.Collections;

public class SchemaHelper {
    public static void updateSchema(Mutation mutation) {
        //Schema.instance.merge(Collections.singleton(mutation));
    }
}
