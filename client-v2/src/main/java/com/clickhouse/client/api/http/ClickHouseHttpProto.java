package com.clickhouse.client.api.http;

public class ClickHouseHttpProto {


    public static final String HEADER_SRV_DISPLAY_NAME = "X-ClickHouse-Server-Display-Name";

    /**
     * Response only header to indicate a query id
     * Cannot be used in request.
     */
    public static final String HEADER_QUERY_ID = "X-ClickHouse-Query-Id";

    public static final String HEADER_SRV_SUMMARY = "X-ClickHouse-Summary";

    /**
     * Response only header to indicate the format of the data.
     * Cannot be used in request.
     */
    public static final String HEADER_FORMAT = "X-ClickHouse-Format";

    public static final String HEADER_TIMEZONE = "X-ClickHouse-Timezone";

    /**
     * Response only header to indicate the error code.
     * Cannot be used in request.
     */
    public static final String HEADER_EXCEPTION_CODE = "X-ClickHouse-Exception-Code";

    /**
     * Response only header to indicate a query progress.
     * Cannot be used in request.
     */
    public static final String HEADER_PROGRESS = "X-ClickHouse-Progress";


    /**
     * Name of default database to be used if not specified in a table name.
     */
    public static final String HEADER_DATABASE = "X-ClickHouse-Database";

    /**
     * Name of user to be used to authenticate
     */
    public static final String HEADER_DB_USER = "X-ClickHouse-User";

    /**
     * Password of user to be used to authenticate. Note: header value should be unencoded, so using
     * special characters might cause issues. It is recommended to use the Basic Authentication instead.
     */
    public static final String HEADER_DB_PASSWORD = "X-ClickHouse-Key";

    public static final String HEADER_SSL_CERT_AUTH = "x-clickhouse-ssl-certificate-auth";

    /**
     * Query parameter to specify the query ID.
     */
    public static final String QPARAM_QUERY_ID = "query_id";

    public static final String QPARAM_ROLE = "role";

    /**
     * Query statement string ( ex. {@code INSERT INTO mytable }
     */
    public static final String QPARAM_QUERY_STMT = "query";

    public static final int DEFAULT_HTTP_PORT = 8123;

    public static final int DEFAULT_HTTPS_PORT = 8443;
}
