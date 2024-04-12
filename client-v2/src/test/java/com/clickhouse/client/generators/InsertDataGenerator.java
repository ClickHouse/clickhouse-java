package com.clickhouse.client.generators;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class InsertDataGenerator {
    public static InputStream generateSimpleRowBinaryData() {
        return new InputStream() {//Placeholder
            @Override
            public int read() throws IOException {
                return 0;
            }
        };
    }

    public static List<Object> generateSimplePOJOs() {
        return new ArrayList<>();//Placeholder
    }
}
