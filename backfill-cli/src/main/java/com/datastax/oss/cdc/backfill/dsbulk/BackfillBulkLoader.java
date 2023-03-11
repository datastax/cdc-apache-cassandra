package com.datastax.oss.cdc.backfill.dsbulk;

import com.datastax.oss.dsbulk.runner.CleanupThread;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader;
import com.datastax.oss.dsbulk.runner.ErrorHandler;
import com.datastax.oss.dsbulk.runner.ExitStatus;
import com.datastax.oss.dsbulk.runner.WorkflowThread;
import com.datastax.oss.dsbulk.runner.cli.CommandLineParser;
import com.datastax.oss.dsbulk.runner.cli.GlobalHelpRequestException;
import com.datastax.oss.dsbulk.runner.cli.ParsedCommandLine;
import com.datastax.oss.dsbulk.runner.cli.SectionHelpRequestException;
import com.datastax.oss.dsbulk.runner.cli.VersionRequestException;
import com.datastax.oss.dsbulk.runner.help.HelpEmitter;
import com.datastax.oss.dsbulk.workflow.api.Workflow;
import com.datastax.oss.dsbulk.workflow.api.utils.WorkflowUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigResolveOptions;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;

/**
 * A class loader aware version of {@link DataStaxBulkLoader}.
 */
public class BackfillBulkLoader extends DataStaxBulkLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackfillBulkLoader.class);

    private final String[] args;
    private final ClassLoader configClassLoader;

    public BackfillBulkLoader(ClassLoader configClassLoader, String... args) {
        super(args);
        this.args = args;
        this.configClassLoader = configClassLoader;
    }

    @NonNull
    public ExitStatus run() {
        Workflow workflow = null;

        try {
            BackfillBulkLoaderCommandLineParser parser = new BackfillBulkLoaderCommandLineParser(configClassLoader, this.args);
            BackfillParsedCommandLine result = parser.parse();
            Config config = result.getConfig();
            //shortcut the the: workflow = result.getWorkflowProvider().newWorkflow(config);
            workflow = new BackfillUnloadWorkflow(configClassLoader, config);
            WorkflowThread workflowThread = new WorkflowThread(workflow);
            Runtime.getRuntime().addShutdownHook(new CleanupThread(workflow, workflowThread));
            workflowThread.start();
            workflowThread.join();
            return workflowThread.getExitStatus();
        } catch (BackfillGlobalHelpRequestException var7) {
            HelpEmitter.emitGlobalHelp(var7.getConnectorName());
            return ExitStatus.STATUS_OK;
        } catch (SectionHelpRequestException var8) {
            SectionHelpRequestException e = var8;

            try {
                HelpEmitter.emitSectionHelp(e.getSectionName(), e.getConnectorName());
                return ExitStatus.STATUS_OK;
            } catch (Exception var6) {
                LOGGER.error(var6.getMessage(), var6);
                return ExitStatus.STATUS_CRASHED;
            }
        } catch (VersionRequestException var9) {
            PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(System.out, Charset.defaultCharset())));
            pw.println(WorkflowUtils.getBulkLoaderNameAndVersion());
            pw.flush();
            return ExitStatus.STATUS_OK;
        } catch (Throwable var10) {
            return ErrorHandler.handleUnexpectedError(workflow, var10);
        }
    }
}
