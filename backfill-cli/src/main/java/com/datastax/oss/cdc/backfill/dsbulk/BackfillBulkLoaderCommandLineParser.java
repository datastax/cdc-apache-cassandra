/**
 * Copyright DataStax, Inc 2021.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datastax.oss.cdc.backfill.dsbulk;

import com.datastax.oss.driver.shaded.guava.common.collect.BiMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.config.ConfigUtils;
import com.datastax.oss.dsbulk.config.shortcuts.ShortcutsFactory;
import com.datastax.oss.dsbulk.io.IOUtils;
import com.datastax.oss.dsbulk.runner.cli.AnsiConfigurator;
import com.datastax.oss.dsbulk.runner.cli.GlobalHelpRequestException;
import com.datastax.oss.dsbulk.runner.cli.ParseException;
import com.datastax.oss.dsbulk.runner.cli.ParsedCommandLine;
import com.datastax.oss.dsbulk.runner.cli.SectionHelpRequestException;
import com.datastax.oss.dsbulk.runner.cli.VersionRequestException;
import com.datastax.oss.dsbulk.runner.utils.StringUtils;
import com.datastax.oss.dsbulk.workflow.api.WorkflowProvider;
import com.datastax.oss.dsbulk.workflow.api.config.ConfigPostProcessor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

/**
 * a class loader aware version of {@link com.datastax.oss.dsbulk.runner.cli.CommandLineParser}.
 */
public class BackfillBulkLoaderCommandLineParser {
    private static final ConfigParseOptions COMMAND_LINE_ARGUMENTS;
    private static final Set<String> COMMON_DRIVER_LIST_TYPE_SETTINGS;
    private final List<String> args;

    public final ClassLoader configClassLoader;

    public BackfillBulkLoaderCommandLineParser(ClassLoader configClassLoader, String... args) {
        this.args = Lists.newArrayList(args);
        this.configClassLoader = configClassLoader;
    }

    public BackfillParsedCommandLine parse() throws ParseException, BackfillGlobalHelpRequestException, SectionHelpRequestException, VersionRequestException {
        if (this.args.isEmpty()) {
            throw new BackfillGlobalHelpRequestException();
        } else {
            this.checkVersionRequest();
            AnsiConfigurator.configureAnsi(this.args);
            Path applicationPath = this.resolveApplicationPath();
            String connectorName = this.resolveConnectorName();
            this.checkHelpRequest(connectorName);
            ConfigFactory.invalidateCaches();
            Config referenceConfig = BackfillConfigUtil.createReferenceConfig(configClassLoader);
            Config applicationConfig = BackfillConfigUtil.createApplicationConfig(applicationPath, configClassLoader);
            BiMap<String, String> shortcuts = ShortcutsFactory.createShortcutsMap(referenceConfig, connectorName == null ? "csv" : connectorName);
            WorkflowProvider workflowProvider = this.resolveWorkflowType();
            Config finalConfig = this.parseArguments(referenceConfig, applicationConfig, shortcuts).resolve();
            finalConfig = this.postProcess(finalConfig);
            return new BackfillParsedCommandLine(workflowProvider, finalConfig);
        }
    }

    @Nullable
    private Path resolveApplicationPath() throws ParseException {
        Iterator<String> iterator = this.args.iterator();

        String arg;
        do {
            if (!iterator.hasNext()) {
                return null;
            }

            arg = (String)iterator.next();
        } while(!arg.equals("-f"));

        if (iterator.hasNext()) {
            iterator.remove();
            Path applicationPath = ConfigUtils.resolvePath((String)iterator.next());
            iterator.remove();
            IOUtils.assertAccessibleFile(applicationPath, "Application file");
            return applicationPath;
        } else {
            throw new ParseException("Expecting application configuration path after -f");
        }
    }

    @Nullable
    private String resolveConnectorName() throws ParseException {
        Iterator<String> iterator = this.args.iterator();

        String arg;
        do {
            if (!iterator.hasNext()) {
                return null;
            }

            arg = (String)iterator.next();
        } while(!arg.equals("-c") && !arg.equals("--connector.name") && !arg.equals("--dsbulk.connector.name"));

        if (iterator.hasNext()) {
            return (String)iterator.next();
        } else {
            throw new ParseException("Expecting connector name after " + arg);
        }
    }

    private void checkVersionRequest() throws VersionRequestException {
        if (!this.args.isEmpty() && ("-v".equals(this.args.get(0)) || "--version".equals(this.args.get(0)))) {
            throw new VersionRequestException();
        }
    }

    private void checkHelpRequest(@Nullable String connectorName) throws ParseException, SectionHelpRequestException {
        ListIterator<String> it = this.args.listIterator();

        for(boolean firstArg = true; this.skipConnector(it); firstArg = false) {
            String arg = (String)it.next();
            boolean helpAsCommand = "help".equals(arg) && firstArg;
            boolean helpAsLongOption = "--help".equals(arg);
            if (helpAsCommand || helpAsLongOption) {
                if (this.skipConnector(it)) {
                    String sectionName = this.sanitizeSectionName((String)it.next());
                    throw new SectionHelpRequestException(sectionName, connectorName);
                } else {
                    throw new ParseException("Expecting section name after " + arg);
                }
            }
        }

    }

    private boolean skipConnector(ListIterator<String> it) {
        if (it.hasNext()) {
            String arg = (String)it.next();
            if (!arg.equals("-c") && !arg.equals("--connector.name") && !arg.equals("--dsbulk.connector.name")) {
                it.previous();
            } else {
                it.next();
            }
        }

        return it.hasNext();
    }

    @NonNull
    private String sanitizeSectionName(@NonNull String sectionName) {
        if (sectionName.startsWith("driver")) {
            sectionName = sectionName.replaceFirst("driver", "datastax-java-driver");
        } else if (!sectionName.startsWith("datastax-java-driver") && !sectionName.startsWith("dsbulk")) {
            sectionName = "dsbulk." + sectionName;
        }

        return sectionName;
    }

    @NonNull
    private WorkflowProvider resolveWorkflowType() throws ParseException {
        System.out.println("this.getAvailableCommands(): " + this.getAvailableCommands());
        System.out.println("args: " + args);
        if (!this.args.isEmpty()) {
            String workflowTypeStr = (String)this.args.remove(0);
            ServiceLoader<WorkflowProvider> loader = ServiceLoader.load(WorkflowProvider.class, configClassLoader);
            Iterator var3 = loader.iterator();

            while(var3.hasNext()) {
                WorkflowProvider workflowProvider = (WorkflowProvider)var3.next();
                if (workflowProvider.getTitle().equals(workflowTypeStr.toLowerCase())) {
                    return workflowProvider;
                }
            }
        }

        throw new ParseException(String.format("First argument must be subcommand \"%s\", or \"help\"", this.getAvailableCommands()));
    }

    @NonNull
    private Config postProcess(Config config) {
        ServiceLoader<ConfigPostProcessor> loader = ServiceLoader.load(ConfigPostProcessor.class, configClassLoader);

        ConfigPostProcessor processor;
        for(Iterator var3 = loader.iterator(); var3.hasNext(); config = processor.postProcess(config)) {
            processor = (ConfigPostProcessor)var3.next();
        }

        return config;
    }

    @NonNull
    private String getAvailableCommands() {
        // Use the classloader that loaded the Config class to load the WorkflowProvider implementations.
        ServiceLoader<WorkflowProvider> loader = ServiceLoader.load(WorkflowProvider.class, configClassLoader);
        List<String> commands = new ArrayList<>();
        for (WorkflowProvider workflowProvider : loader) {
            commands.add(workflowProvider.getTitle().toLowerCase());
        }
        return String.join("\", \"", commands);
    }

    @NonNull
    private Config parseArguments(
            Config referenceConfig, Config currentConfig, Map<String, String> shortcuts)
            throws ParseException {
        Iterator<String> iterator = args.iterator();
        int argumentIndex = 0;
        while (iterator.hasNext()) {
            String arg = iterator.next();
            argumentIndex++;
            try {
                String optionName = null;
                String optionValue = null;
                boolean wasLongOptionWithValue = false;
                try {
                    if (arg.startsWith("--")) {
                        // First, try long option in the form : --key=value; this is not the recommended
                        // way to input long options for dsbulk, but it has been supported since the
                        // beginning. Parse the option using TypeSafe Config since it has to be valid.
                        // We will sanitize the parsed value after.
                        Config config = ConfigFactory.parseString(arg.substring(2));
                        Map.Entry<String, ConfigValue> entry = config.entrySet().iterator().next();
                        optionName = longNameToOptionName(entry.getKey(), referenceConfig);
                        optionValue = entry.getValue().unwrapped().toString();
                        wasLongOptionWithValue = true;
                    }
                } catch (ConfigException.Parse ignored) {
                    // could not parse option, assume it wasn't a long option with value and fall through
                }
                if (!wasLongOptionWithValue) {
                    // Now try long or short option :
                    // --long.option.name value
                    // -shortcut value
                    optionName = parseLongOrShortOptionName(arg, shortcuts, referenceConfig);
                    if (iterator.hasNext()) {
                        // there must be at least one remaining argument containing the option value
                        optionValue = iterator.next();
                        argumentIndex++;
                    } else {
                        throw new ParseException("Expecting value after: " + arg);
                    }
                }
                ConfigValueType optionType = getOptionType(optionName, referenceConfig);
                if (optionType == ConfigValueType.OBJECT) {
                    // the argument we are about to parse is a map (object), we want it to replace the
                    // default value, not to be merged with it, so remove the path from the application
                    // config.
                    currentConfig = currentConfig.withoutPath(optionName);
                }
                String sanitizedOptionValue = sanitizeValue(optionValue, optionType);
                // Pre-validate that optionName and sanitizedOptionValue are syntactically valid, throw
                // immediately if something is not right to get a clear error message specifying the
                // incriminated argument. This won't validate if the value is semantically correct (i.e.
                // if value is of expected type): semantic validation is expected to occur later on in any
                // of the *Settings classes.
                try {
                    // A hack to make the origin description contain the label "Command line argument"
                    // followed by the line number, which corresponds to each argument index.
                    String paddingBeforeOptionName =
                            StringUtils.nCopies("\n", argumentIndex - (wasLongOptionWithValue ? 0 : 1));
                    String paddingBeforeOptionValue = wasLongOptionWithValue ? "" : "\n";
                    currentConfig =
                            ConfigFactory.parseString(
                                            paddingBeforeOptionName
                                                    + optionName
                                                    + '='
                                                    + paddingBeforeOptionValue
                                                    + sanitizedOptionValue,
                                            COMMAND_LINE_ARGUMENTS)
                                    .withFallback(currentConfig);
                } catch (ConfigException e) {
                    IllegalArgumentException cause = ConfigUtils.convertConfigException(e, "");
                    if (optionType == null) {
                        throw new ParseException(
                                String.format("Invalid value for %s: '%s'", optionName, optionValue), cause);
                    } else {
                        throw new ParseException(
                                String.format(
                                        "Invalid value for %s, expecting %s, got: '%s'",
                                        optionName, optionType, optionValue),
                                cause);
                    }
                }
            } catch (RuntimeException e) {
                throw new ParseException("Could not parse argument: " + arg, e);
            }
        }
        return currentConfig;
    }

    @Nullable
    private ConfigValueType getOptionType(String path, Config referenceConfig) {
        if (COMMON_DRIVER_LIST_TYPE_SETTINGS.contains(path)) {
            return ConfigValueType.LIST;
        } else {
            ConfigValueType type = null;

            try {
                type = ConfigUtils.getValueType(referenceConfig, path);
            } catch (ConfigException.Missing var5) {
            }

            return type;
        }
    }

    @NonNull
    private String parseLongOrShortOptionName(String arg, Map<String, String> shortcuts, Config referenceConfig) throws ParseException {
        String optionName;
        String shortcut;
        if (arg.startsWith("--") && arg.length() > 2) {
            shortcut = arg.substring(2);
            optionName = this.longNameToOptionName(shortcut, referenceConfig);
        } else {
            if (!arg.startsWith("-") || arg.length() <= 1) {
                throw new ParseException(String.format("Expecting long or short option, got: '%s'", arg));
            }

            shortcut = arg.substring(1);
            optionName = this.shortcutToOptionName(arg, shortcut, shortcuts);
        }

        return optionName;
    }

    @NonNull
    private String longNameToOptionName(String longName, Config referenceConfig) {
        String optionName;
        if (longName.startsWith("dsbulk.")) {
            optionName = longName;
        } else if (longName.startsWith("datastax-java-driver.")) {
            optionName = longName;
        } else if (longName.startsWith("driver.")) {
            if (this.isDeprecatedDriverSetting(referenceConfig, longName)) {
                optionName = "dsbulk." + longName;
            } else {
                optionName = longName.replaceFirst("driver\\.", "datastax-java-driver.");
            }
        } else {
            optionName = "dsbulk." + longName;
        }

        return optionName;
    }

    @NonNull
    private String shortcutToOptionName(String arg, String shortcut, Map<String, String> shortcuts) throws ParseException {
        if (!shortcuts.containsKey(shortcut)) {
            throw new ParseException("Unknown short option: " + arg);
        } else {
            return (String)shortcuts.get(shortcut);
        }
    }

    private boolean isDeprecatedDriverSetting(Config referenceConfig, String settingName) {
        return referenceConfig.getConfig("dsbulk").hasPathOrNull(settingName);
    }

    @NonNull
    private String sanitizeValue(String optionValue, ConfigValueType optionType) {
        String formatted = optionValue;
        if (optionType == ConfigValueType.STRING) {
            formatted = StringUtils.ensureQuoted(optionValue);
        } else if (optionType == ConfigValueType.LIST) {
            formatted = StringUtils.ensureBrackets(optionValue);
        } else if (optionType == ConfigValueType.OBJECT) {
            formatted = StringUtils.ensureBraces(optionValue);
        }

        return formatted;
    }

    static {
        COMMAND_LINE_ARGUMENTS = ConfigParseOptions.defaults().setOriginDescription("command line argument").setSyntax(ConfigSyntax.CONF);
        COMMON_DRIVER_LIST_TYPE_SETTINGS = ImmutableSet.of("datastax-java-driver.basic.contact-points", "datastax-java-driver.advanced.ssl-engine-factory.cipher-suites", "datastax-java-driver.advanced.metrics.session.enabled", "datastax-java-driver.advanced.metrics.node.enabled", "datastax-java-driver.advanced.metadata.schema.refreshed-keyspaces");
    }
}

