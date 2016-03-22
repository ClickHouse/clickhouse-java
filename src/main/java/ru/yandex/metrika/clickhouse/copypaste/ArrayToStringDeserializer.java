package ru.yandex.metrika.clickhouse.copypaste;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * И все массивы, прилетевшие из кликхауса, станут строками.
 *
 * @author lemmsh
 * @since 7/25/14
 */

class ArrayToStringDeserializer extends JsonDeserializer<List<String>> {

    // cache не concurrent, потому что ничего страшного, что посчитаем несколько раз
    private static final Map<DeserializationContext, JsonDeserializer<Object>> deserializers = new WeakHashMap<DeserializationContext, JsonDeserializer<Object>>();

    @Override
    public List<String> deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        JsonDeserializer<Object> deserializer = deserializers.get(ctxt);
        if (deserializer == null) {
            deserializer = ctxt.findContextualValueDeserializer(TypeFactory.defaultInstance()
                    .constructType(new TypeReference<List<Object>>() {
                    }), null);
            deserializers.put(ctxt, deserializer);
        }

        final Object deserialized = deserializer.deserialize(jp, ctxt);
        if (!(deserialized instanceof List)){
            throw new IllegalStateException();
        }
        //noinspection unchecked
        final List<Object> deserializedList = (List) deserialized;
        List<String> result = new ArrayList<String>();
        for (Object x : deserializedList) {
            String v = null;
            if (x != null) {
                if (x instanceof List) {
                    try {
                        v = new ObjectMapper().writeValueAsString(x);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    v = x.toString();
                }
            }
            result.add(v);
        }
        return result;
    }

}
