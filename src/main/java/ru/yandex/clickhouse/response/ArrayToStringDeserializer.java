package ru.yandex.clickhouse.response;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;


class ArrayToStringDeserializer extends JsonDeserializer<List<String>> {

    private static final LoadingCache<DeserializationContext, JsonDeserializer<Object>> deserializers
            = CacheBuilder.newBuilder()
            .weakKeys()
            .concurrencyLevel(16)
            .maximumSize(10000)
            .build(new CacheLoader<DeserializationContext, JsonDeserializer<Object>>() {
        @Override
        public JsonDeserializer<Object> load(DeserializationContext ctxt) throws Exception {
            return  ctxt.findContextualValueDeserializer(TypeFactory.defaultInstance()
                    .constructType(new TypeReference<List<Object>>() {
                    }), null);
        }
    });

    @Override
    public List<String> deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        JsonDeserializer<Object> deserializer;
        try {
             deserializer = deserializers.get(ctxt);
        } catch (ExecutionException e){
            throw new RuntimeException(e);
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
