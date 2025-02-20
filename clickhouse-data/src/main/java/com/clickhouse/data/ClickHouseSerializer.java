package com.clickhouse.data;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Functional interface for serializtion.
 */
@Deprecated
@FunctionalInterface
public interface ClickHouseSerializer {
    static class CompositeSerializer implements ClickHouseSerializer {
        protected final ClickHouseSerializer[] serializers;

        protected CompositeSerializer(ClickHouseSerializer[] serializers) {
            this.serializers = serializers;
        }

        @Override
        public void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException {
            for (int i = 0, len = serializers.length; i < len; i++) {
                serializers[i].serialize(value, output);
            }
        }
    }

    /**
     * Default serializer simply does nothing.
     */
    static ClickHouseSerializer DO_NOTHING = (v, o) -> {
    };

    /**
     * Default deserializer throws IOException to inform caller serialization is
     * not supported.
     */
    static ClickHouseSerializer NOT_SUPPORTED = (v, o) -> {
        throw new IOException("Serialization is not supported");
    };

    static String TYPE_NAME = "Serializer";

    /**
     * Creates composite serializer.
     *
     * @param first first serializer
     * @param more  other serializers
     * @return composite serializer
     */
    static ClickHouseSerializer of(ClickHouseSerializer first, ClickHouseSerializer... more) {
        List<ClickHouseSerializer> list = new LinkedList<>();
        if (first != null) {
            list.add(first);
        }

        if (more != null) {
            for (int i = 0, len = more.length; i < len; i++) {
                ClickHouseSerializer s = more[i];
                if (s != null) {
                    list.add(s);
                }
            }
        }
        if (list.isEmpty()) {
            return DO_NOTHING;
        }

        return new CompositeSerializer(list.toArray(new ClickHouseSerializer[0]));
    }

    static ClickHouseSerializer of(List<ClickHouseSerializer> list) {
        if (list == null) {
            return DO_NOTHING;
        }

        for (Iterator<ClickHouseSerializer> it = list.iterator(); it.hasNext();) {
            ClickHouseSerializer s = it.next();
            if (s == null) {
                it.remove();
            }
        }

        return list.isEmpty() ? DO_NOTHING : new CompositeSerializer(list.toArray(new ClickHouseSerializer[0]));
    }

    /**
     * Writes serialized value to output stream.
     *
     * @param value  non-null value to be serialized
     * @param output non-null output stream
     * @throws IOException when failed to write data to output stream
     */
    void serialize(ClickHouseValue value, ClickHouseOutputStream output) throws IOException;
}
