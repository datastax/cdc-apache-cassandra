package com.datastax.oss.cdc.backfill.dsbulk;

import com.datastax.oss.dsbulk.workflow.api.WorkflowProvider;
import com.typesafe.config.Config;

public class BackfillParsedCommandLine {
    private final WorkflowProvider workflowProvider;
    private final Config config;

    BackfillParsedCommandLine(WorkflowProvider workflowProvider, Config config) {
        this.workflowProvider = workflowProvider;
        this.config = config;
    }

    public WorkflowProvider getWorkflowProvider() {
        return this.workflowProvider;
    }

    public Config getConfig() {
        return this.config;
    }
}
