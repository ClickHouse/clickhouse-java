package com.clickhouse.client.api.internal;


/**
 * Incomplete list of server side settings.
 * This class is not intended to list all possible settings, but only those that are commonly used.
 *
 */
public final class ServerSettings {

    public static final String WAIT_END_OF_QUERY = "wait_end_of_query";

    // -- Experimental features --

    /**
     * Server will expect a string in JSON format and parse it into a JSON object.
     */
    public static final String INPUT_FORMAT_BINARY_READ_JSON_AS_STRING = "input_format_binary_read_json_as_string";

    /**
     * Server will return a JSON object as a string.
     */
    public static final String OUTPUT_FORMAT_BINARY_WRITE_JSON_AS_STRING = "output_format_binary_write_json_as_string";

    /**
     * Limit number of rows in a result set
     */
    public static final String MAX_RESULT_ROWS = "max_result_rows";

    /**
     * Defines server response if result set exceeded a limit set by {@code max_result_rows}.
     * Possible values are 'throw' or 'break'. Default is 'throw'
     */
    public static final String RESULT_OVERFLOW_MODE = "result_overflow_mode";

    public static final String ASYNC_INSERT = "async_insert";

    public static final String WAIT_ASYNC_INSERT = "wait_for_async_insert";
}
