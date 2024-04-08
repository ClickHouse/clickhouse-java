package com.clickhouse.client.sample;

import com.clickhouse.client.annotation.Column;
import com.clickhouse.client.annotation.Table;
import com.clickhouse.data.ClickHouseDataType;

@Table
public class SamplePersonPOJO {
    @Column(index = 0, type = ClickHouseDataType.String)
    private String name;
    @Column(index = 1, type = ClickHouseDataType.UInt8)
    private int age;
    @Column(index = 2, type = ClickHouseDataType.String)
    private String address;

    public SamplePersonPOJO() {
    }

    public SamplePersonPOJO(String name, int age, String address) {
        this.name = name;
        this.age = age;
        this.address = address;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }
}
