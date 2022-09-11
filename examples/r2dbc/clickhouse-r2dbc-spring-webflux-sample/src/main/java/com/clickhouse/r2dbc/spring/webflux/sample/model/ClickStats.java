package com.clickhouse.r2dbc.spring.webflux.sample.model;


import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.LocalDate;

public class ClickStats {
    private String domain;
    private String path;
    @JsonFormat(pattern = "yyyy-MM-dd")
    private LocalDate cdate;
    private long count;

    public ClickStats(String domain, String path, LocalDate date, long count) {
        this.domain = domain;
        this.path = path;
        this.cdate = date;
        this.count = count;
    }

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

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public LocalDate getCdate() {
        return cdate;
    }

    public void setCdate(LocalDate cdate) {
        this.cdate = cdate;
    }
}
