package com.clickhouse.client.query;


public class SimplePOJO {

    long id = 0;

    String name = null;

    Integer age = null;

    Boolean bool = false;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Boolean getBool() {
        return bool;
    }

    public void setBool(Boolean bool) {
        this.bool = bool;
    }

    @Override
    public String toString() {
        return "SimplePOJO{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", bool=" + bool +
                '}';
    }
}
