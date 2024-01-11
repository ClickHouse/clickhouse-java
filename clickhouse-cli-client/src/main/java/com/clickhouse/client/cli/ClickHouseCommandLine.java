package com.clickhouse.client.cli;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseCredentials;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.cli.config.ClickHouseCommandLineOption;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.config.ClickHouseSslMode;
import com.clickhouse.data.ClickHouseChecker;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseDataConfig;
import com.clickhouse.data.ClickHouseExternalTable;
import com.clickhouse.data.ClickHouseFile;
import com.clickhouse.data.ClickHouseInputStream;
import com.clickhouse.data.ClickHouseOutputStream;
import com.clickhouse.data.ClickHousePassThruStream;
import com.clickhouse.data.ClickHouseUtils;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;
// deprecate from version 0.6.0
@Deprecated
public class ClickHouseCommandLine implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseCommandLine.class);

    public static final String DEFAULT_CLI_ARG_VERSION = "--version";
    public static final String DEFAULT_CLICKHOUSE_CLI_PATH = "clickhouse";
    public static final String DEFAULT_CLIENT_OPTION = "client";
    public static final String DEFAULT_LOCAL_OPTION = "local";
    public static final String DEFAULT_DOCKER_CLI_PATH = "docker";
    public static final String DEFAULT_DOCKER_IMAGE = "clickhouse/clickhouse-server";

    public static final boolean DEFAULT_CLI_IS_AVAILALBE;
    public static final boolean DEFAULT_DOCKER_IS_AVAILALBE;

    private static final Map<String, Boolean> cache = Collections.synchronizedMap(new WeakHashMap<>(8));

    static {
        String option = DEFAULT_CLIENT_OPTION;
        int timeout = (int) ClickHouseClientOption.CONNECTION_TIMEOUT.getEffectiveDefaultValue();

        DEFAULT_CLI_IS_AVAILALBE = check(timeout, DEFAULT_CLICKHOUSE_CLI_PATH, option, DEFAULT_CLI_ARG_VERSION);
        DEFAULT_DOCKER_IS_AVAILALBE = check(timeout, DEFAULT_DOCKER_CLI_PATH, option, DEFAULT_CLI_ARG_VERSION);
    }

    static String getCommandLine(ClickHouseConfig config, String option) {
        int timeout = config.getConnectionTimeout();
        String cli = config.getStrOption(ClickHouseCommandLineOption.CLICKHOUSE_CLI_PATH);
        if (ClickHouseChecker.isNullOrBlank(cli)) {
            cli = DEFAULT_CLI_IS_AVAILALBE ? DEFAULT_CLICKHOUSE_CLI_PATH : null;
        } else if (!check(timeout, cli, option, DEFAULT_CLI_ARG_VERSION)) {
            cli = null;
        }

        if (cli == null) {
            cli = config.getStrOption(ClickHouseCommandLineOption.DOCKER_CLI_PATH);
            if (ClickHouseChecker.isNullOrBlank(cli)) {
                cli = DEFAULT_DOCKER_IS_AVAILALBE ? DEFAULT_DOCKER_CLI_PATH : null;
            } else if (!check(timeout, cli, option, DEFAULT_CLI_ARG_VERSION)) {
                cli = null;
            }
        }
        return cli;
    }

    static boolean check(int timeout, String command, String... args) {
        if (ClickHouseChecker.isNullOrBlank(command) || args == null) {
            throw new IllegalArgumentException("Non-blank command and non-null arguments are required");
        }

        StringBuilder builder = new StringBuilder(command);
        for (String str : args) {
            builder.append(' ').append(str);
        }
        String commandLine = builder.toString();
        Boolean value = cache.get(commandLine);
        if (value == null) {
            value = Boolean.FALSE;

            List<String> list = new ArrayList<>(args.length + 1);
            list.add(command);
            Collections.addAll(list, args);
            Process process = null;
            try {
                process = new ProcessBuilder(list).start();
                process.getOutputStream().close();
                if (process.waitFor(timeout, TimeUnit.MILLISECONDS)) {
                    int exitValue = process.exitValue();
                    if (exitValue != 0) {
                        log.trace("Command %s exited with value %d", list, exitValue);
                    }
                    value = exitValue == 0;
                } else {
                    log.trace("Timed out after waiting %d ms for command %s to complete", timeout, list);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.trace("Failed to check command %s due to: %s", list, e.getMessage());
            } finally {
                if (process != null && process.isAlive()) {
                    process.destroyForcibly();
                }
                process = null;
            }

            if (value) { // no negative cache
                cache.put(commandLine, value);
            }
        }

        return Boolean.TRUE.equals(value);
    }

    static void dockerCommand(ClickHouseConfig config, String hostDir, String containerDir, int timeout,
            List<String> commands) {
        String cli = config.getStrOption(ClickHouseCommandLineOption.DOCKER_CLI_PATH);
        boolean useDocker = false;
        if (ClickHouseChecker.isNullOrBlank(cli)) {
            if (DEFAULT_DOCKER_IS_AVAILALBE) {
                cli = DEFAULT_DOCKER_CLI_PATH;
                useDocker = true;
            }
        } else if (check(timeout, cli, DEFAULT_CLI_ARG_VERSION)) {
            useDocker = true;
        }
        if (useDocker) {
            commands.add(cli);
        } else {
            throw new UncheckedIOException(new ConnectException("Docker command-line is not available: " + cli));
        }

        String img = config.getStrOption(ClickHouseCommandLineOption.CLICKHOUSE_DOCKER_IMAGE);
        if (ClickHouseChecker.isNullOrBlank(img)) {
            img = DEFAULT_DOCKER_IMAGE;
        }
        String str = config.getStrOption(ClickHouseCommandLineOption.CLI_CONTAINER_ID);
        if (!ClickHouseChecker.isNullOrBlank(str)) {
            if (!check(timeout, cli, "exec", str, DEFAULT_CLICKHOUSE_CLI_PATH, DEFAULT_CLIENT_OPTION,
                    DEFAULT_CLI_ARG_VERSION)) {
                synchronized (ClickHouseCommandLine.class) {
                    if (!check(timeout, cli, "exec", str, DEFAULT_CLICKHOUSE_CLI_PATH, DEFAULT_CLIENT_OPTION,
                            DEFAULT_CLI_ARG_VERSION)
                            && !check(timeout, cli, "run", "--rm", "--name", str, "-v", hostDir + ':' + containerDir,
                                    "-d", img, "tail", "-f", "/dev/null")) {
                        throw new UncheckedIOException(new ConnectException("Failed to start new container: " + str));
                    }
                }
            }
            // reuse the existing container
            commands.add("exec");
            commands.add("-i");
            commands.add(str);
        } else { // create new container for each query
            if (!check(timeout, cli, "run", "--rm", img, DEFAULT_CLICKHOUSE_CLI_PATH, DEFAULT_CLIENT_OPTION,
                    DEFAULT_CLI_ARG_VERSION)) {
                throw new UncheckedIOException(new ConnectException("Invalid ClickHouse docker image: " + img));
            }
            commands.add("run");
            commands.add("--rm");
            commands.add("-i");
            commands.add("-v");
            commands.add(hostDir + ':' + containerDir);
            commands.add(img);
        }

        commands.add(DEFAULT_CLICKHOUSE_CLI_PATH);
    }

    static Process startProcess(ClickHouseRequest<?> request) {
        final ClickHouseConfig config = request.getConfig();
        final ClickHouseNode server = request.getServer();
        final int timeout = config.getSocketTimeout();

        String hostDir = config.getStrOption(ClickHouseCommandLineOption.CLI_WORK_DIRECTORY);
        hostDir = ClickHouseUtils.normalizeDirectory(
                ClickHouseChecker.isNullOrBlank(hostDir) ? System.getProperty("java.io.tmpdir") : hostDir);
        String containerDir = config.getStrOption(ClickHouseCommandLineOption.CLI_CONTAINER_DIRECTORY);
        if (ClickHouseChecker.isNullOrBlank(containerDir)) {
            containerDir = "/tmp/";
        } else {
            containerDir = ClickHouseUtils.normalizeDirectory(containerDir);
        }

        List<String> commands = new LinkedList<>();
        String cli = config.getStrOption(ClickHouseCommandLineOption.CLICKHOUSE_CLI_PATH);
        boolean useCli = false;
        if (ClickHouseChecker.isNullOrBlank(cli)) {
            if (DEFAULT_CLI_IS_AVAILALBE) {
                cli = DEFAULT_CLICKHOUSE_CLI_PATH;
                useCli = true;
            }
        } else if (check(timeout, cli, DEFAULT_CLIENT_OPTION, DEFAULT_CLI_ARG_VERSION)) {
            useCli = true;
        }
        if (useCli) {
            commands.add(cli);
            containerDir = hostDir;
        } else {
            // fallback to docker
            dockerCommand(config, hostDir, containerDir, timeout, commands);
        }
        commands.add(DEFAULT_CLIENT_OPTION);

        if (config.isSsl()) {
            commands.add("--secure");

            if (config.getSslMode() == ClickHouseSslMode.NONE) {
                commands.add("--accept-invalid-certificate");
            }
        }
        if (config.isResponseCompressed()) {
            commands.add("--compression=1");
            switch (config.getResponseCompressAlgorithm()) {
                case LZ4:
                    commands.add("--network_compression_method=lz4");
                    break;
                case ZSTD:
                    commands.add("--network_compression_method=zstd");
                    if (config.getResponseCompressLevel() >= 0 && config.getResponseCompressLevel() <= 22) {
                        commands.add("----network_zstd_compression_level=" + config.getResponseCompressLevel());
                    }
                    break;
                default:
                    break;
            }
        } else {
            commands.add("--compression=0");
        }

        commands.add("--host=".concat(server.getHost()));
        commands.add("--port=".concat(Integer.toString(server.getPort())));

        String str = server.getDatabase(config);
        if (!ClickHouseChecker.isNullOrBlank(str)) {
            commands.add("--database=".concat(str));
        }
        str = config.getStrOption(ClickHouseCommandLineOption.CLI_CONFIG_FILE);
        if (config.getBoolOption(ClickHouseCommandLineOption.USE_CLI_CONFIG)
                && !ClickHouseChecker.isNullOrBlank(str) && Files.exists(Paths.get(str))) {
            commands.add("--config-file=".concat(str));
        } else {
            ClickHouseCredentials credentials = server.getCredentials(config);
            str = credentials.getUserName();
            if (!ClickHouseChecker.isNullOrBlank(str)) {
                commands.add("--user=".concat(str));
            }
            str = credentials.getPassword();
            if (!ClickHouseChecker.isNullOrBlank(str)) {
                commands.add("--password=".concat(str));
            }
        }
        commands.add("--format=".concat(config.getFormat().name()));

        str = request.getQueryId().orElse("");
        if (!ClickHouseChecker.isNullOrBlank(str)) {
            commands.add("--query_id=".concat(str));
        }
        commands.add("--query=".concat(str = request.getStatements(false).get(0)));

        for (ClickHouseExternalTable table : request.getExternalTables()) {
            ClickHouseFile tableFile = ClickHouseFile.of(table.getContent(), table.getCompression(),
                    table.getCompressionLevel(), table.getFormat());
            commands.add("--external");
            String filePath;
            if (!tableFile.hasOutput() || !tableFile.getFile().getAbsolutePath().startsWith(hostDir)) {
                // creating a hard link is faster but it's not platform-independent
                File f = ClickHouseInputStream.save(
                        Paths.get(hostDir, "chc_".concat(request.getManager().createUniqueId())).toFile(),
                        table.getContent(), config.getWriteBufferSize(), config.getSocketTimeout(), true);
                filePath = containerDir.concat(f.getName());
            } else {
                filePath = tableFile.getFile().getAbsolutePath();
                if (!hostDir.equals(containerDir)) {
                    filePath = Paths.get(containerDir, filePath.substring(hostDir.length())).toFile().getAbsolutePath();
                }
            }
            commands.add("--file=" + filePath);
            if (!ClickHouseChecker.isNullOrEmpty(table.getName())) {
                commands.add("--name=".concat(table.getName()));
            }
            if (table.getFormat() != null) {
                commands.add("--format=".concat(table.getFormat().name()));
            }
            commands.add("--structure=".concat(table.getStructure()));
        }

        Map<String, Serializable> settings = request.getSettings();
        Object value = settings.get("max_result_rows");
        if (value instanceof Number) {
            long maxRows = ((Number) value).longValue();
            if (maxRows > 0L) {
                commands.add("--limit=".concat(Long.toString(maxRows)));
            }
        }
        value = settings.get("result_overflow_mode");
        if (value != null) {
            commands.add("--result_overflow_mode=".concat(value.toString()));
        }
        value = settings.get("readonly");
        if (value != null) {
            commands.add("--readonly=".concat(value.toString()));
        }
        if (config.getBoolOption(ClickHouseCommandLineOption.USE_PROFILE_EVENTS)) {
            commands.add("--print-profile-events");
            commands.add("--profile-events-delay-ms=-1");
        }

        log.debug("Query: %s", str);
        ProcessBuilder builder = new ProcessBuilder(commands);
        String workDirectory = config.getStrOption(
                ClickHouseCommandLineOption.CLI_WORK_DIRECTORY);
        if (!ClickHouseChecker.isNullOrBlank(workDirectory)) {
            Path p = Paths.get(workDirectory);
            if (Files.isDirectory(p)) {
                builder.directory(p.toFile());
            }
        }

        if (request.hasOutputStream()) {
            final ClickHouseOutputStream chOutput = request.getOutputStream().get(); // NOSONAR

            if (chOutput.hasUnderlyingStream()) {
                final ClickHousePassThruStream customStream = chOutput.getUnderlyingStream();
                File f = customStream instanceof ClickHouseFile ? ((ClickHouseFile) customStream).getFile() : null;
                if (f == null) {
                    throw new UncheckedIOException(new IOException("Output file not found in " + customStream));
                }
                if (hostDir.equals(containerDir)) {
                    // builder.redirectOutput(f);
                } else if (f.getAbsolutePath().startsWith(hostDir)) {
                    String relativePath = f.getAbsolutePath().substring(hostDir.length());
                    // FIXME overrided by below
                    builder.redirectOutput(new File(containerDir.concat(relativePath)));
                } else {
                    String fileName = f.getName();
                    int len = fileName.length();
                    int index = fileName.indexOf('.', 1);
                    String uuid = request.getManager().createUniqueId();
                    if (index > 0 && index + 1 < len) {
                        fileName = new StringBuilder(len + uuid.length() + 1).append(fileName.substring(0, index))
                                .append('_').append(uuid).append(fileName.substring(index)).toString();
                    } else {
                        fileName = new StringBuilder(len + uuid.length() + 1).append(fileName).append('_')
                                .append(request.getManager().createUniqueId()).toString();
                    }
                    Path newPath = Paths.get(hostDir, fileName);
                    try {
                        f = Files.createLink(newPath, f.toPath()).toFile();
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    } catch (UnsupportedOperationException e) {
                        try {
                            f = ClickHouseInputStream.save(newPath.toFile(), new FileInputStream(f),
                                    config.getWriteBufferSize(), timeout, true);
                        } catch (FileNotFoundException exp) {
                            throw new UncheckedIOException(exp);
                        }
                    }
                }
                builder.redirectOutput(f);
            }
        }

        final Optional<ClickHouseInputStream> in = request.getInputStream();
        try {
            final Process process;
            if (in.isPresent()) {
                final ClickHouseInputStream chInput = in.get();
                final File inputFile;
                if (chInput.hasUnderlyingStream()) {
                    final ClickHousePassThruStream customStream = chInput.getUnderlyingStream();
                    inputFile = customStream instanceof ClickHouseFile ? ((ClickHouseFile) customStream).getFile()
                            : ClickHouseFile.of(customStream.getInputStream(), config.getRequestCompressAlgorithm(),
                                    config.getRequestCompressLevel(), config.getFormat()).getFile();
                } else {
                    inputFile = ClickHouseFile.of(chInput, config.getRequestCompressAlgorithm(),
                            config.getRequestCompressLevel(), config.getFormat()).getFile();
                }
                process = builder.redirectInput(inputFile).start();
            } else {
                process = builder.start();
                process.getOutputStream().close();
            }
            return process;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private final ClickHouseRequest<?> request;

    private String error;

    private final Process process;

    public ClickHouseCommandLine(ClickHouseRequest<?> request) {
        this.request = request;

        this.error = null;

        this.process = startProcess(request);
    }

    public ClickHouseInputStream getInputStream() throws IOException {
        ClickHouseConfig c = request.getConfig();
        ClickHouseOutputStream out = request.getOutputStream().orElse(null);
        Runnable postCloseAction = () -> {
            IOException exp = getError();
            if (exp != null) {
                throw new UncheckedIOException(exp);
            }
        };
        if (out != null && !out.hasUnderlyingStream()) {
            try (OutputStream o = out) {
                ClickHouseInputStream.pipe(process.getInputStream(), o, c.getWriteBufferSize());
            }
            return ClickHouseInputStream.wrap(null, ClickHouseInputStream.empty(), c.getReadBufferSize(),
                    ClickHouseCompression.NONE, ClickHouseDataConfig.DEFAULT_READ_COMPRESS_LEVEL, postCloseAction);
        } else {
            return ClickHouseInputStream.of(process.getInputStream(), c.getReadBufferSize(), postCloseAction);
        }
    }

    IOException getError() {
        if (error == null) {
            int bufferSize = (int) ClickHouseClientOption.BUFFER_SIZE.getDefaultValue();
            try (ByteArrayOutputStream output = new ByteArrayOutputStream(bufferSize)) {
                ClickHouseInputStream.pipe(process.getErrorStream(), output, bufferSize);
                error = new String(output.toByteArray(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                error = "";
            }
            try {
                int exitValue = process.waitFor();
                if (exitValue != 0) {
                    if (error.isEmpty()) {
                        error = ClickHouseUtils.format("Command exited with value %d", exitValue);
                    } else {
                        int index = error.trim().indexOf('\n');
                        error = index > 0 ? error.substring(index + 1) : error;
                    }
                } else {
                    if (!error.isEmpty()) {
                        // TODO update response summary
                        log.trace(() -> {
                            for (String line : error.split("\n")) {
                                log.trace(line);
                            }
                            return "";
                        });
                    }
                    error = "";
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                process.destroyForcibly();
                throw new CompletionException(e);
            }
        }
        return !ClickHouseChecker.isNullOrBlank(error) ? new IOException(error) : null;
    }

    @Override
    public void close() {
        if (process.isAlive()) {
            process.destroyForcibly();
        }
    }
}
