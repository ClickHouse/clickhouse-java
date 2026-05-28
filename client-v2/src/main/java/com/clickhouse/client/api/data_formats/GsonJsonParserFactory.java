package com.clickhouse.client.api.data_formats;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class GsonJsonParserFactory implements JsonParserFactory {
    private final Gson gson;

    public GsonJsonParserFactory() {
        GsonBuilder builder = new GsonBuilder();
        customize(builder);
        builder.setLenient(); // JSONEachRow needs lenient reader for multiple root objects
        this.gson = builder.create();
    }

    protected void customize(GsonBuilder builder) {
        // JSONEachRow numbers may represent UInt64 or Decimal values, so avoid
        // Gson's default Double materialization and preserve the original value.
        builder.setObjectToNumberStrategy(ToNumberPolicy.BIG_DECIMAL);
    }

    @Override
    public JsonParser createJsonParser(InputStream in) {
        return new JsonParserImpl(gson.newJsonReader(new InputStreamReader(in, StandardCharsets.UTF_8)));
    }

    private class JsonParserImpl implements JsonParser {

        private final JsonReader reader;

        public JsonParserImpl(JsonReader jsonReader) {
            this.reader = jsonReader;
        }


        @Override
        public Map<String, Object> nextRow() throws Exception {
            try {
                if (reader.peek() == JsonToken.END_DOCUMENT) {
                    return null;
                }
            } catch (java.io.EOFException e) {
                return null;
            }

            return GsonJsonParserFactory.this.gson.fromJson(reader, new TypeToken<Map<String, Object>>() {
            }.getType());
        }

        @Override
        public void close() throws Exception {
            reader.close();
        }
    }
}
