package com.clickhouse.demo_service.data;

import com.clickhouse.demo_service.jpa.ClickHouseStringArrayType;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import org.hibernate.annotations.Array;
import org.hibernate.annotations.JdbcType;
import org.hibernate.annotations.Type;
import org.hibernate.type.descriptor.jdbc.ArrayJdbcType;

import java.sql.JDBCType;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.List;

@Entity
@Data
@Table(name = "ui_events")
public class UIEvent {

    @Id
    private String id;

    private Timestamp timestamp;

    private String eventName;

    @JdbcType(ArrayJdbcType.class)
    @Type(ClickHouseStringArrayType.class)
    private Collection<String> tags;
}
