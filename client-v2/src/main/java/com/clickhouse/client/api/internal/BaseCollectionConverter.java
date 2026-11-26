package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClickHouseException;
import com.clickhouse.data.ClickHouseColumn;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * Walks a (possibly nested) collection and writes it into an accumulator.
 * The accumulator and the column are passed to each call, so an instance holds no
 * per-conversion state and can be used by many threads at the same time.
 */
public abstract class BaseCollectionConverter<ACC, LIST> {
    public static final String ARRAY_START = "[";
    public static final String ARRAY_END = "]";

    private final String itemDelimiter;

    protected BaseCollectionConverter(String itemDelimiter) {
        this.itemDelimiter = itemDelimiter;
    }

    protected abstract void append(ACC acc, String str);

    protected abstract String buildString(ACC acc);

    protected abstract void onStart(ListConversionState<LIST> state, ACC acc, ClickHouseColumn column);

    protected abstract void onEnd(ListConversionState<LIST> state, ACC acc, ClickHouseColumn column);

    protected abstract void onItem(Object item, ListConversionState<LIST> state, ACC acc, ClickHouseColumn column);

    protected abstract String onEmptyCollection();

    protected abstract boolean isEmpty(LIST list);

    protected abstract boolean isSubList(Object list);

    protected abstract int listSize(LIST list);

    protected abstract Object getNext(ListConversionState<LIST> state);

    public final String convert(LIST value, ACC acc, ClickHouseColumn column) {
        if (isEmpty(value)) {
            return onEmptyCollection();
        }

        Deque<ListConversionState<LIST>> stack = new ArrayDeque<>();
        ListConversionState<LIST> state = new ListConversionState<>(value, listSize(value));
        while (state != null) {
            if (state.isFirst()) {
                onStart(state, acc, column);
            }
            if (state.hasNext()) {
                Object item = getNext(state);
                state.incPos();
                if (isSubList(item)) {
                    stack.push(state);
                    LIST list = (LIST) item;
                    state = new ListConversionState<>(list, listSize(list));
                } else {
                    onItem(item, state, acc, column);
                    if (state.hasNext()) {
                        append(acc, itemDelimiter);
                    }
                }
            } else {
                onEnd(state, acc, column);
                state = stack.isEmpty() ? null : stack.pop();
                if (state != null && state.hasNext()) {
                    append(acc, itemDelimiter);
                }
            }
        }

        return buildString(acc);
    }

    public static final class ListConversionState<LIST> {

        final LIST list;
        int position;
        int size;

        private ListConversionState(LIST list, int size) {
            this.list = list;
            this.position = 0;
            this.size = size;
        }

        public LIST getList() {
            return list;
        }

        public int getPosition() {
            return position;
        }

        public void incPos() {
            this.position++;
        }

        public boolean hasNext() {
            return position < size;
        }

        public boolean isFirst() {
            return position == 0;
        }
    }

    public abstract static class BaseArrayWriter extends BaseCollectionWriter<Object> {

        protected BaseArrayWriter() {
            super(", ");
        }

        @Override
        protected boolean isEmpty(Object objects) {
            return listSize(objects) == 0;
        }

        @Override
        protected boolean isSubList(Object list) {
            return list != null &&  list.getClass().isArray();
        }

        @Override
        protected int listSize(Object objects) {
            return Array.getLength(objects);
        }

        @Override
        protected Object getNext(ListConversionState<Object> state) {
            return Array.get(state.getList(), state.getPosition());
        }
    }

    public abstract static class BaseListWriter
            extends BaseCollectionWriter<List<?>> {
        protected BaseListWriter() {
            super(", ");
        }

        @Override
        protected boolean isEmpty(List<?> objects) {
            return objects.isEmpty();
        }

        @Override
        protected boolean isSubList(Object list) {
            return list instanceof List<?>;
        }

        @Override
        protected int listSize(List<?> objects) {
            return objects.size();
        }

        @Override
        protected Object getNext(ListConversionState<List<?>> state) {
            return state.getList().get(state.getPosition());
        }
    }

    public abstract static class BaseCollectionWriter<T> extends
            BaseCollectionConverter<Appendable, T> {

        protected BaseCollectionWriter(String itemDelimiter) {
            super(itemDelimiter);
        }

        @Override
        protected void append(Appendable appendable, String str) {
            try {
                appendable.append(str);
            } catch (IOException e) {
                throw new ClickHouseException(e.getMessage(), e);
            }
        }

        @Override
        protected String buildString(Appendable appendable) {
            return appendable.toString();
        }

        @Override
        protected void onStart(ListConversionState<T> state, Appendable appendable, ClickHouseColumn column) {
            try {
                appendable.append(ARRAY_START);
            } catch (IOException e) {
                throw new ClickHouseException(e.getMessage(), e);
            }
        }

        @Override
        protected void onEnd(ListConversionState<T> state, Appendable appendable, ClickHouseColumn column) {
            try {
                appendable.append(ARRAY_END);
            } catch (IOException e) {
                throw new ClickHouseException(e.getMessage(), e);
            }
        }

        @Override
        protected String onEmptyCollection() {
            return ARRAY_START + ARRAY_END;
        }
    }
}
