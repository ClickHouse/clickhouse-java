package com.clickhouse.data;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.clickhouse.data.value.ClickHouseEmptyValue;

/**
 * Functional interface for deserialization.
 */
@Deprecated
@FunctionalInterface
public interface ClickHouseDeserializer {
    static class CompositeDeserializer implements ClickHouseDeserializer {
        protected final ClickHouseDeserializer[] deserializers;

        protected CompositeDeserializer(ClickHouseDeserializer[] deserializers) {
            this.deserializers = deserializers;
        }

        @Override
        public ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException {
            ClickHouseValue v = null;
            for (int i = 0, len = deserializers.length; i < len; i++) {
                v = deserializers[i].deserialize(ref, input);
                if (v != null) {
                    break;
                }
            }
            return v;
        }
    }

    /**
     * Default deserializer simply returns empty value.
     */
    static ClickHouseDeserializer EMPTY_VALUE = (v, i) -> ClickHouseEmptyValue.INSTANCE;

    /**
     * Default deserializer throws IOException to inform caller deserialization is
     * not supported.
     */
    static ClickHouseDeserializer NOT_SUPPORTED = (v, i) -> {
        throw new IOException("Deserialization is not supported");
    };

    static String TYPE_NAME = "Deserializer";

    /**
     * Creates composite deserializer.
     *
     * @param first first deserializer
     * @param more  other deserializers
     * @return composite deserializer
     */
    static ClickHouseDeserializer of(ClickHouseDeserializer first, ClickHouseDeserializer... more) {
        List<ClickHouseDeserializer> list = new LinkedList<>();
        if (first != null) {
            list.add(first);
        }

        if (more != null) {
            for (int i = 0, len = more.length; i < len; i++) {
                ClickHouseDeserializer d = more[i];
                if (d != null) {
                    list.add(d);
                }
            }
        }
        if (list.isEmpty()) {
            return EMPTY_VALUE;
        }

        return new CompositeDeserializer(list.toArray(new ClickHouseDeserializer[0]));
    }

    static ClickHouseDeserializer of(List<ClickHouseDeserializer> list) {
        if (list == null) {
            return EMPTY_VALUE;
        }

        for (Iterator<ClickHouseDeserializer> it = list.iterator(); it.hasNext();) {
            ClickHouseDeserializer d = it.next();
            if (d == null) {
                it.remove();
            }
        }

        return list.isEmpty() ? EMPTY_VALUE : new CompositeDeserializer(list.toArray(new ClickHouseDeserializer[0]));
    }

    /**
     * Deserializes data read from input stream.
     *
     * @param ref   wrapper object can be reused, could be null(always return new
     *              wrapper object)
     * @param input non-null input stream
     * @return deserialized value which usually is same as {@code ref}, return null
     *         if the deserialization expects more
     * @throws IOException when failed to read data from input stream
     */
    ClickHouseValue deserialize(ClickHouseValue ref, ClickHouseInputStream input) throws IOException;
}
