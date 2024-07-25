package com.clickhouse.client.http;

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
     * Query parameter to specify the query ID.
     */
    public static final String QPARAM_QUERY_ID = "query_id";

}
