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

}
