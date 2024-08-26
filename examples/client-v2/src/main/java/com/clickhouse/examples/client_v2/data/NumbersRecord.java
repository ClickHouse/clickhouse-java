package com.clickhouse.demo_service.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigInteger;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NumbersRecord {

    private UUID id;

    private long p1;

    private BigInteger number;

    private float p2;

    private double p3;
}
