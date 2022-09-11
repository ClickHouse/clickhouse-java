package com.clickhouse.r2dbc.spring.webflux.sample.model;


public class Click {

    String domain;
    String path;

    public String getDomain() {
        return domain;
    }

    public String getPath() {
        return path;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
