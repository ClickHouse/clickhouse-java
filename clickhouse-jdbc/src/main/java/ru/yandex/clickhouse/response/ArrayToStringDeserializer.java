package ru.yandex.clickhouse.response;

import com.clickhouse.client.ClickHouseCache;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.type.TypeFactory;
import ru.yandex.clickhouse.Jackson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class ArrayToStringDeserializer extends JsonDeserializer<List<String>> {
    private static final ClickHouseCache<DeserializationContext, JsonDeserializer<Object>> deserializers = ClickHouseCache.create(1000, 300,
            (ctx) -> {
                try {
                    return ctx.findContextualValueDeserializer(
                            TypeFactory.defaultInstance().constructType(new TypeReference<List<Object>>() {
                            }), null);
                } catch (JsonMappingException e) {
                    throw new IllegalStateException(e);
                }
            });

    @Override
    public List<String> deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        JsonDeserializer<Object> deserializer = deserializers.get(ctxt);

        final Object deserialized = deserializer.deserialize(jp, ctxt);
        if (!(deserialized instanceof List)) {
            throw new IllegalStateException();
        }
        // noinspection unchecked
        final List<Object> deserializedList = (List) deserialized;
        List<String> result = new ArrayList<String>();
        for (Object x : deserializedList) {
            String v = null;
            if (x instanceof List) {
                try {
                    v = Jackson.getObjectMapper().writeValueAsString(x);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            } else if (x != null) {
                v = x.toString();
            }
            result.add(v);
        }
        return result;
    }

}
