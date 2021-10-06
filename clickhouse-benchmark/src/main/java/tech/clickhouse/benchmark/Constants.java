package tech.clickhouse.benchmark;

/**
 * Constant interface.
 */
public interface Constants {
    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final String DEFAULT_DB = "system";
    public static final String DEFAULT_USER = "default";
    public static final String DEFAULT_PASSWD = "";

    public static final int GRPC_PORT = 9100;
    public static final int HTTP_PORT = 8123;
    public static final int MYSQL_PORT = 9004;
    public static final int NATIVE_PORT = 9000;
    public static final int POSTGRESQL_PORT = 9005;

    public static final String NORMAL_STATEMENT = "normal";
    public static final String PREPARED_STATEMENT = "prepared";

    public static final String REUSE_CONNECTION = "reuse";
    public static final String NEW_CONNECTION = "new";
}
