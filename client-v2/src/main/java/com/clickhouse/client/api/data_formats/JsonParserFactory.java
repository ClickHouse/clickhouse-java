package com.clickhouse.client.api.data_formats;

import java.io.IOException;
import java.io.InputStream;

public interface JsonParserFactory {


    /**
     * Implementation should create only instance of actual JSON parser.
     * This method is called for each request and should avoid long initialization or
     * create big objects
     * @param in - stream of bytes to parse as JSON
     * @return instance of {@link JsonParser}
     */
    JsonParser createJsonParser(InputStream in) throws IOException;
}
