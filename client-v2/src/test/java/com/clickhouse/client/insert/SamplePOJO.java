package com.clickhouse.client.insert;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Random;
import java.util.random.RandomGenerator;

public class SamplePOJO {
    private int id;
    private String name;
    private double value;

    public SamplePOJO() {
        this.id = RandomGenerator.getDefault().nextInt();
        this.name = RandomStringUtils.randomAlphanumeric(16);
        this.value = RandomGenerator.getDefault().nextDouble();
    }

    public SamplePOJO(int id, String name, double value) {
        this.id = id;
        this.name = name;
        this.value = value;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public double getValue() {
        return value;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
