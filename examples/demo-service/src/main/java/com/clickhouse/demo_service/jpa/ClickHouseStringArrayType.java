package com.clickhouse.demo_service.jpa;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.usertype.UserType;

import java.io.Serializable;
import java.sql.*;
import java.util.*;

public class ClickHouseStringArrayType implements UserType<Collection> {
    @Override
    public int getSqlType() {
        return Types.ARRAY;
    }

    @Override
    public Class<Collection> returnedClass() {
        return Collection.class;
    }

    @Override
    public boolean equals(Collection x, Collection y) {
        return x.equals(y);
    }

    @Override
    public int hashCode(Collection x) {
        return x.hashCode();
    }

    @Override
    public Collection nullSafeGet(ResultSet rs, int position, SharedSessionContractImplementor session, Object owner) throws SQLException {
        // This implementation not optimal, but portable.
        Array array = rs.getArray(position);
        if (array != null) {
            List<Object> list = new ArrayList<>();
            for (Object i : (Object[]) array.getArray() ) {
                list.add(i);
            }
            return list;
        }
        return Collections.emptyList();
    }

    @Override
    public void nullSafeSet(PreparedStatement st, Collection value, int index, SharedSessionContractImplementor session) throws SQLException {
        st.setObject(index, value);
    }

    @Override
    public Collection deepCopy(Collection value) {
        return new ArrayList(value);
    }

    @Override
    public boolean isMutable() {
        // value should not be changed
        return false;
    }

    @Override
    public Serializable disassemble(Collection value) {
        return new ArrayList<>(value);
    }

    @Override
    public Collection assemble(Serializable cached, Object owner) {
        return (Collection) cached;
    }

}
