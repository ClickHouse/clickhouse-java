package com.clickhouse.client.api.data_formats.internal;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.Map;

public class JacksonJsonParser implements com.clickhouse.client.api.data_formats.internal.JsonParser {
    private final ObjectMapper mapper;
    private final JsonFactory factory;
    private com.fasterxml.jackson.core.JsonParser parser;

    public JacksonJsonParser(InputStream inputStream) {
        this.mapper = new ObjectMapper();
        this.factory = new JsonFactory();
        try {
            this.parser = factory.createParser(inputStream);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Jackson parser", e);
        }
    }

    @Override
    public Map<String, Object> nextRow() throws Exception {
        if (parser.nextToken() == null) {
            return null;
        }
        if (parser.currentToken() != JsonToken.START_OBJECT) {
            // Handle cases where there might be extra characters between objects,
            // like newlines in JSONEachRow.
            while (parser.nextToken() != null && parser.currentToken() != JsonToken.START_OBJECT) {
                // skip
            }
            if (parser.currentToken() == null) {
                return null;
            }
        }
        return mapper.readValue(parser, Map.class);
    }

    @Override
    public void close() throws Exception {
        if (parser != null) {
            parser.close();
        }
    }
}
