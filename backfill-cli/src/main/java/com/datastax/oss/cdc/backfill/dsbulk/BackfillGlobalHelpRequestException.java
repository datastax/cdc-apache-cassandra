package com.datastax.oss.cdc.backfill.dsbulk;

import edu.umd.cs.findbugs.annotations.Nullable;

public class BackfillGlobalHelpRequestException extends Exception {
    private final String connectorName;

    BackfillGlobalHelpRequestException() {
        this((String)null);
    }

    BackfillGlobalHelpRequestException(@Nullable String connectorName) {
        this.connectorName = connectorName;
    }

    @Nullable
    public String getConnectorName() {
        return this.connectorName;
    }
}
