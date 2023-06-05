/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.ignite.internal.commandline;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgument;
import org.apache.ignite.internal.commandline.argument.parser.CLIArgumentParser;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.ArgumentGroup;
import org.apache.ignite.internal.management.api.CliPositionalSubcommands;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ssl.SslContextFactory;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ENABLE_EXPERIMENTAL_COMMAND;
import static org.apache.ignite.internal.client.GridClientConfiguration.DFLT_PING_INTERVAL;
import static org.apache.ignite.internal.client.GridClientConfiguration.DFLT_PING_TIMEOUT;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_HOST;
import static org.apache.ignite.internal.commandline.CommandHandler.DFLT_PORT;
import static org.apache.ignite.internal.commandline.CommandHandler.UTILITY_NAME;
import static org.apache.ignite.internal.commandline.argument.parser.CLIArgument.optionalArg;
import static org.apache.ignite.internal.management.api.CommandUtils.CMD_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.PARAMETER_PREFIX;
import static org.apache.ignite.internal.management.api.CommandUtils.PARAM_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.asOptional;
import static org.apache.ignite.internal.management.api.CommandUtils.fromFormattedCommandName;
import static org.apache.ignite.internal.management.api.CommandUtils.isBoolean;
import static org.apache.ignite.internal.management.api.CommandUtils.parameterExample;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedFieldName;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedNames;
import static org.apache.ignite.internal.management.api.CommandUtils.visitCommandParams;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_SSL_PROTOCOL;

/**
 * Common argument parser.
 * Also would parse high-level command and delegate parsing for its argument to the command.
 */
public class CommonArgParser {
    /** */
    private final IgniteLogger logger;

    /** */
    private final Map<String, Command<?, ?>> cmds;

    /** */
    static final String CMD_HOST = "--host";

    /** */
    static final String CMD_PORT = "--port";

    /** */
    static final String CMD_PASSWORD = "--password";

    /** */
    static final String CMD_USER = "--user";

    /** Option is used for auto confirmation. */
    public static final String CMD_AUTO_CONFIRMATION = "--yes";

    /** Ping interval for grid client. See {@link GridClientConfiguration#getPingInterval()}. */
    static final String CMD_PING_INTERVAL = "--ping-interval";

    /** Ping timeout for grid client. See {@link GridClientConfiguration#getPingTimeout()}. */
    static final String CMD_PING_TIMEOUT = "--ping-timeout";

    /** Verbose mode. */
    public static final String CMD_VERBOSE = "--verbose";

    // SSL configuration section

    /** */
    static final String CMD_SSL_PROTOCOL = "--ssl-protocol";

    /** */
    static final String CMD_SSL_KEY_ALGORITHM = "--ssl-key-algorithm";

    /** */
    static final String CMD_SSL_CIPHER_SUITES = "--ssl-cipher-suites";

    /** */
    static final String CMD_KEYSTORE = "--keystore";

    /** */
    static final String CMD_KEYSTORE_PASSWORD = "--keystore-password";

    /** */
    static final String CMD_KEYSTORE_TYPE = "--keystore-type";

    /** */
    static final String CMD_TRUSTSTORE = "--truststore";

    /** */
    static final String CMD_TRUSTSTORE_PASSWORD = "--truststore-password";

    /** */
    static final String CMD_TRUSTSTORE_TYPE = "--truststore-type";

    /** */
    static final String CMD_ENABLE_EXPERIMENTAL = "--enable-experimental";

    /** */
    static final String CMD_SSL_FACTORY = "--ssl-factory";

    /** Set of sensitive arguments */
    private static final Set<String> SENSITIVE_ARGUMENTS = new HashSet<>();

    /** */
    private static final BiConsumer<String, Integer> PORT_VALIDATOR = (name, val) -> {
        if (val <= 0 || val > 65535)
            throw new IllegalArgumentException("Invalid value for " + name + ": " + val);
    };

    /** */
    private static final BiConsumer<String, Long> POSITIVE_LONG = (name, val) -> {
        if (val <= 0)
            throw new IllegalArgumentException("Invalid value for " + name + ": " + val);
    };

    /** */
    private final List<CLIArgument<?>> common = new ArrayList<>();

    static {
        SENSITIVE_ARGUMENTS.add(CMD_PASSWORD);
        SENSITIVE_ARGUMENTS.add(CMD_KEYSTORE_PASSWORD);
        SENSITIVE_ARGUMENTS.add(CMD_TRUSTSTORE_PASSWORD);
    }

    /**
     * @param arg To check.
     * @return True if provided argument is among sensitive one and not should be displayed.
     */
    public static boolean isSensitiveArgument(String arg) {
        return SENSITIVE_ARGUMENTS.contains(arg);
    }

    /**
     * @param logger Logger.
     * @param cmds Supported commands.
     */
    public CommonArgParser(IgniteLogger logger, Map<String, Command<?, ?>> cmds) {
        this.logger = logger;
        this.cmds = cmds;

        BiConsumer<String, String> securityWarn = (name, val) -> logger.info(String.format("Warning: %s is insecure. " +
                "Whenever possible, use interactive prompt for password (just discard %s option).", val, val));

        arg(CMD_HOST, "HOST_OR_IP", String.class, DFLT_HOST);
        arg(CMD_PORT, "PORT", Integer.class, DFLT_PORT, PORT_VALIDATOR);
        arg(CMD_USER, "USER", String.class, null);
        arg(CMD_PASSWORD, "PASSWORD", String.class, null, securityWarn);
        arg(CMD_PING_INTERVAL, "PING_INTERVAL", Long.class, DFLT_PING_INTERVAL, POSITIVE_LONG);
        arg(CMD_PING_TIMEOUT, "PING_TIMEOUT", Long.class, DFLT_PING_TIMEOUT, POSITIVE_LONG);
        arg(CMD_VERBOSE, CMD_VERBOSE, boolean.class, false);
        arg(CMD_SSL_PROTOCOL, "SSL_PROTOCOL[, SSL_PROTOCOL_2, ..., SSL_PROTOCOL_N]", String[].class, new String[] {DFLT_SSL_PROTOCOL});
        arg(CMD_SSL_CIPHER_SUITES, "SSL_CIPHER_1[, SSL_CIPHER_2, ..., SSL_CIPHER_N]", String[].class, U.EMPTY_STRS);
        arg(CMD_SSL_KEY_ALGORITHM, "SSL_KEY_ALGORITHM", String.class, SslContextFactory.DFLT_KEY_ALGORITHM);
        arg(CMD_SSL_FACTORY, "SSL_FACTORY_PATH", String.class, null);
        arg(CMD_KEYSTORE_TYPE, "KEYSTORE_TYPE", String.class, SslContextFactory.DFLT_STORE_TYPE);
        arg(CMD_KEYSTORE, "KEYSTORE_PATH", String.class, null);
        arg(CMD_KEYSTORE_PASSWORD, "KEYSTORE_PASSWORD", String.class, null, securityWarn);
        arg(CMD_TRUSTSTORE_TYPE, "TRUSTSTORE_TYPE", String.class, SslContextFactory.DFLT_STORE_TYPE);
        arg(CMD_TRUSTSTORE, "TRUSTSTORE_PATH", String.class, null);
        arg(CMD_TRUSTSTORE_PASSWORD, "TRUSTSTORE_PASSWORD", String.class, null, securityWarn);
        arg(CMD_AUTO_CONFIRMATION, CMD_AUTO_CONFIRMATION, boolean.class, false);
        arg(
            CMD_ENABLE_EXPERIMENTAL,
            CMD_ENABLE_EXPERIMENTAL, Boolean.class,
            IgniteSystemProperties.getBoolean(IGNITE_ENABLE_EXPERIMENTAL_COMMAND)
        );
    }

    /** */
    private <T> void arg(String name, String usage, Class<T> type, T dflt, BiConsumer<String, T> validator) {
        common.add(optionalArg(name, usage, type, t -> dflt, validator));
    }

    /** */
    private <T> void arg(String name, String usage, Class<T> type, T dflt) {
        common.add(optionalArg(name, usage, type, () -> dflt));
    }

    /**
     * Creates list of common utility options.
     *
     * @return Array of common utility options.
     */
    public String[] getCommonOptions() {
        List<String> list = new ArrayList<>();

        for (CLIArgument<?> arg : common) {
            if (arg.name().equals(CMD_AUTO_CONFIRMATION))
                continue;

            if (isBoolean(arg.type()))
                list.add(asOptional(arg.name(), true));
            else
                list.add(asOptional(arg.name() + " " + arg.usage(), true));
        }

        return list.toArray(U.EMPTY_STRS);
    }

    /**
     * Parses and validates arguments.
     *
     * @param raw Raw arguments.
     * @return Arguments bean.
     * @throws IllegalArgumentException In case arguments aren't valid.
     */
    <A extends IgniteDataTransferObject> ConnectionAndSslParameters<A> parseAndValidate(
        List<String> raw
    ) {
        List<String> args = new ArrayList<>(raw);

        Command<A, ?> cmd = command(args.iterator());

        IgniteBiTuple<List<CLIArgument<?>>, List<CLIArgument<?>>> cmdArgs = commandArguments(cmd);

        List<CLIArgument<?>> allNamedArgs = new ArrayList<>(common);

        allNamedArgs.addAll(cmdArgs.get1());

        CLIArgumentParser parser = new CLIArgumentParser(allNamedArgs, cmdArgs.get2());

        parser.parse(args.iterator());

        A arg = argument(
            cmd.argClass(),
            (fld, pos) -> parser.get(pos),
            fld -> parser.get(toFormattedFieldName(fld))
        );

        if (!parser.<Boolean>get(CMD_ENABLE_EXPERIMENTAL) && cmd.experimental()) {
            logger.warning(
                String.format("To use experimental command add " + CMD_ENABLE_EXPERIMENTAL + " parameter for %s",
                    UTILITY_NAME)
            );

            throw new IllegalArgumentException("Experimental commands disabled");
        }

        return new ConnectionAndSslParameters<>(cmd, arg, parser);
    }

    /**
     * Get command from hierarchical root.
     *
     * @param iter Iterator of commands names.
     * @return Command to execute.
     * @param <A> Argument type.
     */
    protected <A extends IgniteDataTransferObject> Command<A, ?> command(
        Iterator<String> iter
    ) {
        Command<?, ?> cmd = null;

        while (iter.hasNext() && cmd == null)
            cmd = cmds.get(iter.next());

        if (cmd == null)
            throw new IllegalArgumentException("No action was specified");

        // Remove command name parameter to exclude it from ongoing parsing.
        iter.remove();

        while (cmd instanceof CommandsRegistry && iter.hasNext()) {
            String name = iter.next();

            char delim = PARAM_WORDS_DELIM;

            if (!cmd.getClass().isAnnotationPresent(CliPositionalSubcommands.class)) {
                if (!name.startsWith(PARAMETER_PREFIX))
                    break;

                name = name.substring(PARAMETER_PREFIX.length());

                delim = CMD_WORDS_DELIM;
            }

            Command<A, ?> cmd1 =
                (Command<A, ?>)((CommandsRegistry<?, ?>)cmd).command(fromFormattedCommandName(name, delim));

            if (cmd1 == null)
                break;

            cmd = cmd1;

            // Remove command name parameter to exclude it from ongoing parsing.
            iter.remove();
        }

        return (Command<A, ?>)cmd;
    }

    /** */
    private static IgniteBiTuple<List<CLIArgument<?>>, List<CLIArgument<?>>> commandArguments(Command<?, ?> cmd) {
        List<CLIArgument<?>> namedArgs = new ArrayList<>();
        List<CLIArgument<?>> positionalArgs = new ArrayList<>();

        BiFunction<Field, Boolean, CLIArgument<?>> toArg = (fld, optional) -> new CLIArgument<>(
            toFormattedFieldName(fld),
            null,
            optional,
            fld.getType(),
            null,
            (name, val) -> {}
        );

        ArgumentGroup argGrp = cmd.argClass().getAnnotation(ArgumentGroup.class);
        Set<String> grpdFlds = argGrp == null
            ? Collections.emptySet()
            : new HashSet<>(Arrays.asList(argGrp.value()));

        Consumer<Field> namedArgCb = fld -> namedArgs.add(
            toArg.apply(fld, grpdFlds.contains(fld.getName()) || fld.getAnnotation(Argument.class).optional())
        );

        Consumer<Field> positionalArgCb = fld -> positionalArgs.add(new CLIArgument<>(
            fld.getName(),
            null,
            fld.getAnnotation(Argument.class).optional(),
            fld.getType(),
            null,
            (name, val) -> {}
        ));

        BiConsumer<ArgumentGroup, List<Field>> argGrpCb = (argGrp0, flds) -> flds.forEach(fld -> {
            if (fld.isAnnotationPresent(Positional.class))
                positionalArgCb.accept(fld);
            else
                namedArgCb.accept(fld);
        });

        visitCommandParams(cmd.argClass(), positionalArgCb, namedArgCb, argGrpCb);

        return F.t(namedArgs, positionalArgs);
    }

    /**
     * Fill and vaildate command argument.
     *
     * @param argCls Argument class.
     * @param positionalParamProvider Provider of positional parameters.
     * @param paramProvider Provider of named parameters.
     * @return Argument filled with parameters.
     * @param <A> Argument type.
     */
    private static <A extends IgniteDataTransferObject> A argument(
        Class<A> argCls,
        BiFunction<Field, Integer, Object> positionalParamProvider,
        Function<Field, Object> paramProvider
    ) {
        try {
            ArgumentState<A> arg = new ArgumentState<>(argCls);

            visitCommandParams(
                argCls,
                fld -> arg.accept(fld, positionalParamProvider.apply(fld, arg.nextIdx())),
                fld -> arg.accept(fld, paramProvider.apply(fld)),
                (argGrp, flds) -> flds.forEach(fld -> {
                    if (fld.isAnnotationPresent(Positional.class))
                        arg.accept(fld, positionalParamProvider.apply(fld, arg.nextIdx()));
                    else
                        arg.accept(fld, paramProvider.apply(fld));
                })
            );

            if (arg.argGrp != null && (!arg.grpOptional() && !arg.grpFldExists))
                throw new IllegalArgumentException("One of " + toFormattedNames(arg.grpdFlds) + " required");

            return arg.res;
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    private static class ArgumentState<A extends IgniteDataTransferObject> implements BiConsumer<Field, Object> {
        /** */
        final A res;

        /** */
        final ArgumentGroup argGrp;

        /** */
        boolean grpFldExists;

        /** */
        int idx;

        /** */
        final Set<String> grpdFlds;

        /** */
        public ArgumentState(Class<A> argCls) throws InstantiationException, IllegalAccessException {
            res = argCls.newInstance();
            argGrp = argCls.getAnnotation(ArgumentGroup.class);
            grpdFlds = argGrp == null
                ? Collections.emptySet()
                : new HashSet<>(Arrays.asList(argGrp.value()));
        }

        /** */
        public boolean grpOptional() {
            return argGrp == null || argGrp.optional();
        }

        /** */
        private int nextIdx() {
            int idx0 = idx;

            idx++;

            return idx0;
        }

        /** {@inheritDoc} */
        @Override public void accept(Field fld, Object val) {
            boolean grpdFld = grpdFlds.contains(fld.getName());

            if (val == null) {
                if (grpdFld || fld.getAnnotation(Argument.class).optional())
                    return;

                String name = fld.isAnnotationPresent(Positional.class)
                    ? parameterExample(fld, false)
                    : toFormattedFieldName(fld);

                throw new IllegalArgumentException("Argument " + name + " required.");
            }

            if (grpdFld) {
                if (grpFldExists && (argGrp != null && argGrp.onlyOneOf()))
                    throw new IllegalArgumentException("Only one of " + toFormattedNames(grpdFlds) + " allowed");

                grpFldExists = true;
            }

            try {
                res.getClass().getMethod(fld.getName(), fld.getType()).invoke(res, val);
            }
            catch (NoSuchMethodException | IllegalAccessException e) {
                throw new IgniteException(e);
            }
            catch (InvocationTargetException e) {
                if (e.getTargetException() != null && e.getTargetException() instanceof RuntimeException)
                    throw (RuntimeException)e.getTargetException();
            }
        }
    }
}
