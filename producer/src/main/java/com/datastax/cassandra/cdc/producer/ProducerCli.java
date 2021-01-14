package com.datastax.cassandra.cdc.producer;

import io.micronaut.context.ApplicationContext;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "cdcpub",
        description = "CDC publisher",
        mixinStandardHelpOptions = true)
public class ProducerCli implements Callable<Object> {

    @CommandLine.Option(names = {"-v", "--verbose"}, description = "Verbose mode")
    boolean verbose;

    public static void main(String... args) {

        // 1. parse the command line
        CommandLine top = new CommandLine(new ProducerCli())
                .setCaseInsensitiveEnumValuesAllowed(true);

        CommandLine.ParseResult parsedCommands = null;
        try {
            parsedCommands = top.parseArgs(args);
        } catch(CommandLine.ParameterException ex) {
            // 2. handle incorrect user input for one of the subcommands
            System.err.println(ex.getMessage());
            ex.getCommandLine().usage(System.err);
            System.exit(1);
        }

        // 3. check if the user requested help
        for(CommandLine parsed : parsedCommands.asCommandLineList()) {
            if(parsed.isUsageHelpRequested()) {
                parsed.usage(System.err);
                System.exit(0);
            } else if(parsed.isVersionHelpRequested()) {
                parsed.printVersionHelp(System.err);
                System.exit(0);
            }
        }


        // 4. execute the most specific subcommand
        Object last = parsedCommands.asCommandLineList().get(parsedCommands.asCommandLineList().size() - 1).getCommand();
        if(last instanceof Runnable) {
            ((Runnable) last).run();
        } else if(last instanceof Callable) {

            try {
                Object result = ((Callable) last).call();
            } catch(Exception e) {
                e.printStackTrace();
            }
            // ... do something with result
        }
        System.exit(0);
    }

    @Override
    public Object call() throws Exception {
        try (ApplicationContext context = ApplicationContext.run()) {
            context.getBean(CDCPublisher.class).publish();
        }
        return null;
    }
}
