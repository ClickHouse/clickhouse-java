package com.clickhouse.demo_service.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.math.BigInteger;
import java.util.UUID;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class VirtualDatasetRecord {

    private UUID id;

    private long p1;

    private BigInteger number;

    private float p2;

    private double p3;
}
