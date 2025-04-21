package com.clickhouse.client.insert;

import com.clickhouse.client.ClientTests;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

@Getter
@Setter
public class SimplePOJO {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimplePOJO.class);
    private int int32;
    private String str;

    public SimplePOJO() {
        long seed = System.currentTimeMillis();
        final Random random = new Random(seed);
        this.int32 = random.nextInt();
        this.str = RandomStringUtils.randomAlphabetic(1, 256);
    }

    public static String generateTableCreateSQL(String tableName) {
        return "CREATE TABLE " + tableName + " (" +
                "int32 Int32, " +
                "str String, " +
                "int64 Int64 MATERIALIZED abs(toInt64(int32)), " +
                ") ENGINE = MergeTree ORDER BY ()";
    }

}
