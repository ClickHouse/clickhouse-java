package com.clickhouse.client.api.data_formats.internal;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.Map;

public class JacksonJsonParser implements com.clickhouse.client.api.data_formats.internal.JsonParser {
    private final ObjectMapper mapper;
    private final com.fasterxml.jackson.core.JsonParser parser;

    public JacksonJsonParser(InputStream inputStream) {
        this.mapper = new ObjectMapper();
        try {
            this.parser = new JsonFactory().createParser(inputStream);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Jackson parser", e);
        }
    }

    @Override
    public Map<String, Object> nextRow() throws Exception {
        // Jackson's streaming parser skips whitespace (including the newlines that
        // separate JSONEachRow objects), so reaching EOF is the only reason
        // nextToken() returns null. Any non-START_OBJECT token here would indicate
        // malformed input and is reported by mapper.readValue(...).
        if (parser.nextToken() == null) {
            return null;
        }
        return mapper.readValue(parser, Map.class);
    }

    @Override
    public void close() throws Exception {
        parser.close();
    }
}
