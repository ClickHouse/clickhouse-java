package com.clickhouse.client.cli.config;

import java.io.Serializable;

import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseChecker;
@Deprecated (since = "0.6.0", forRemoval = true)
public enum ClickHouseCommandLineOption implements ClickHouseOption {
    /**
     * ClickHouse native command-line client path. Empty value is treated as
     * 'clickhouse'.
     */
    CLICKHOUSE_CLI_PATH("clickhouse_cli_path", "",
            "ClickHouse native command-line client path, empty value is treated as 'clickhouse'"),
    /**
     * ClickHouse docker image. Empty value is treated as
     * 'clickhouse/clickhouse-server'.
     */
    CLICKHOUSE_DOCKER_IMAGE("clickhouse_docker_image", "clickhouse/clickhouse-server",
            "ClickHouse docker image, empty value is treated as 'clickhouse/clickhouse-server'"),
    /**
     * Docker command-line path. Empty value is treated as 'docker'.
     */
    DOCKER_CLI_PATH("docker_cli_path", "", "Docker command-line path, empty value is treated as 'docker'"),
    /**
     * ClickHouse native command-line client configuration file. Empty value will
     * disable {@link #USE_CLI_CONFIG}.
     */
    CLI_CONFIG_FILE("cli_config_file", "~/.clickhouse-client/config.xml",
            "ClickHouse native command-line client configuration file, empty value will disable 'use_cli_config'"),
    /**
     * Docker container ID or name. Empty value will result in new container being
     * created for each query.
     */
    CLI_CONTAINER_ID("cli_container_id", "clickhouse-cli-client",
            "Docker container ID or name, empty value will result in new container being created for each query"),
    /**
     * Work directory inside container, only works running in docker mode(when
     * {@link #CLICKHOUSE_CLI_PATH} is not available). Empty value is treated as
     * '/tmp'.
     */
    CLI_CONTAINER_DIRECTORY("cli_container_directory", "",
            "Work directory inside container, empty value is treated as '/tmp'"),
    /**
     * Command-line work directory. Empty value is treated as system temporary
     * directory(e.g. {@code System.getProperty("java.io.tmpdir")}). When running in
     * docker mode, it's mounted as {@link #CLI_CONTAINER_DIRECTORY} in container.
     */
    CLI_WORK_DIRECTORY("cli_work_directory", "",
            "Command-line work directory, empty value is treate as system temporary directory"),
    /**
     * Whether to use native command-line client configuration file as defined in
     * {@link #CLI_CONFIG_FILE}.
     */
    USE_CLI_CONFIG("use_cli_config", false,
            "Whether to use native command-line client configuration file as defined in 'cli_config_file'"),
    /**
     * Whether to use profile events or not. This is needed when you want to get
     * meaningful response summary.
     */
    USE_PROFILE_EVENTS("use_profile_events", false, "Whether to use profile events or not");

    private final String key;
    private final Serializable defaultValue;
    private final Class<? extends Serializable> clazz;
    private final String description;
    private final boolean sensitive;

    <T extends Serializable> ClickHouseCommandLineOption(String key, T defaultValue, String description) {
        this(key, defaultValue, description, false);
    }

    <T extends Serializable> ClickHouseCommandLineOption(String key, T defaultValue, String description,
            boolean sensitive) {
        this.key = ClickHouseChecker.nonNull(key, "key");
        this.defaultValue = ClickHouseChecker.nonNull(defaultValue, "defaultValue");
        this.clazz = defaultValue.getClass();
        this.description = ClickHouseChecker.nonNull(description, "description");
        this.sensitive = sensitive;
    }

    @Override
    public Serializable getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public Class<? extends Serializable> getValueType() {
        return clazz;
    }

    @Override
    public boolean isSensitive() {
        return sensitive;
    }
}
