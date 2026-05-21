package com.clickhouse.client.api.data_formats;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class JacksonJsonParserFactory implements JsonParserFactory {
    private final ObjectMapper mapper;

    public JacksonJsonParserFactory() {
        this.mapper = createMapper();
    }

    protected ObjectMapper createMapper() {
        // override this method to customize object mapper
        return new ObjectMapper();
    }

    @Override
    public JsonParser createJsonParser(InputStream in) throws IOException {
        return new JsonParserImpl(mapper.createParser(in));
    }

    private class JsonParserImpl implements JsonParser {

        private final com.fasterxml.jackson.core.JsonParser parser;

        public JsonParserImpl(com.fasterxml.jackson.core.JsonParser parser) {
            this.parser = parser;
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
}
