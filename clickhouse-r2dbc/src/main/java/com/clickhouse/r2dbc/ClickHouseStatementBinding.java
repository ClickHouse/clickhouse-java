package com.clickhouse.r2dbc;

import java.util.ArrayList;
import java.util.List;

class ClickHouseStatementBinding {
    public static final String NOT_ALL_PARAMETERS_ARE_SET = "Not all parameters are set.";
    List<Binding> bindingList;
    Binding current;
    int size;

    ClickHouseStatementBinding(int size) {
        this.size = size;
        current = new Binding(size);
        bindingList = new ArrayList<>();
    }

    void addBinding(int index, Object value) {
        current.setParam(index, value);
    }

    void add() {
        if (current.isCompleted()) {
            bindingList.add(current);
            current = new Binding(size);
        }

    }

    List<Binding> getBoundList() {
        List<Binding> bindingList = (this.bindingList == null) ? new ArrayList<>() : this.bindingList;
        if (current.values.length > 0 && current.isCompleted())
            bindingList.add(current);
        return bindingList;
    }

    public static class Binding {

        Object[] values;

        private Binding(int size) {
            values = new Object[size];
        }

        private void setParam(int index, Object value) {
            values[index] = value;
        }

        private boolean isCompleted(){
            for (Object value: values) {
                if (value == null) throw new IllegalStateException(NOT_ALL_PARAMETERS_ARE_SET);
            }
            return true;
        }
    }
}

