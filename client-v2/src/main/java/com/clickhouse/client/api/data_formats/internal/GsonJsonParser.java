package com.clickhouse.client.api.data_formats.internal;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class GsonJsonParser implements JsonParser {
    private final Gson gson;
    private final JsonReader reader;

    public GsonJsonParser(InputStream inputStream) {
        this.gson = new Gson();
        this.reader = new JsonReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        this.reader.setLenient(true); // JSONEachRow needs lenient reader for multiple root objects
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
        return gson.fromJson(reader, new TypeToken<Map<String, Object>>() {}.getType());
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }
}
