package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.ClickHouseException;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

public abstract class BaseCollectionConverter<TAcc, TList> {
    public static final String ARRAY_START = "[";
    public static final String ARRAY_END = "]";

    private final String itemDelimiter;

    protected BaseCollectionConverter(String itemDelimiter) {
        this.itemDelimiter = itemDelimiter;
    }

    protected abstract void setAccumulator(TAcc acc);

    protected abstract void append(String str);

    protected abstract String buildString();

    protected abstract void onStart(ListConversionState<TList> state);

    protected abstract void onEnd(ListConversionState<TList> state);

    protected abstract void onItem(Object item, ListConversionState<TList> state);

    protected abstract String onEmptyCollection();

    protected abstract boolean isEmpty(TList list);

    protected abstract boolean isSubList(Object list);

    protected abstract int listSize(TList list);

    protected abstract Object getNext(ListConversionState<TList> state);

    public final String convert(TList value, TAcc acc) {
        if (isEmpty(value)) {
            return onEmptyCollection();
        }
        setAccumulator(acc);

        Deque<ListConversionState<TList>> stack = new ArrayDeque<>();
        ListConversionState<TList> state = new ListConversionState<>(value, listSize(value));
        while (state != null) {
            if (state.isFirst()) {
                onStart(state);
            }
            if (state.hasNext()) {
                Object item = getNext(state);
                state.incPos();
                if (isSubList(item)) {
                    stack.push(state);
                    TList list = (TList) item;
                    state = new ListConversionState<>(list, listSize(list));
                } else {
                    onItem(item, state);
                    if (state.hasNext()) {
                        append(itemDelimiter);
                    }
                }
            } else {
                onEnd(state);
                state = stack.isEmpty() ? null : stack.pop();
                if (state != null && state.hasNext()) {
                    append(itemDelimiter);
                }
            }
        }

        return buildString();
    }

    public static final class ListConversionState<TList> {

        final TList list;
        int position;
        int size;

        public ListConversionState(TList list, int size) {
            this.list = list;
            this.position = 0;
            this.size = size;
        }

        public TList getList() {
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

    public static abstract class BaseArrayWriter extends BaseCollectionWriter<Object> {

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

    public static abstract class BaseListWriter
            extends BaseCollectionWriter<List<?>> {
        public BaseListWriter() {
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

    public static abstract class BaseCollectionWriter<T> extends
            BaseCollectionConverter<Appendable, T> {

        protected Appendable appendable;

        protected BaseCollectionWriter(String itemDelimiter) {
            super(itemDelimiter);
        }

        @Override
        protected void setAccumulator(Appendable appendable) {
            this.appendable = appendable;
        }

        @Override
        protected void append(String str) {
            try {
                appendable.append(str);
            } catch (IOException e) {
                throw new ClickHouseException(e.getMessage(), e);
            }
        }

        @Override
        protected String buildString() {
            return appendable.toString();
        }

        @Override
        protected void onStart(ListConversionState<T> state) {
            try {
                appendable.append(ARRAY_START);
            } catch (IOException e) {
                throw new ClickHouseException(e.getMessage(), e);
            }
        }

        @Override
        protected void onEnd(ListConversionState<T> state) {
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
