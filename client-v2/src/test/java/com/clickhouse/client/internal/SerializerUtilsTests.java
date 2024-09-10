package com.clickhouse.client.internal;

import com.clickhouse.client.api.internal.SerializerUtils;
import com.clickhouse.client.api.query.POJOSetter;
import com.clickhouse.client.query.SamplePOJO;
import com.clickhouse.data.ClickHouseColumn;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SerializerUtilsTests {

    @Test(enabled = false)
    public void testDeserialize() throws Exception {

        Map<String, POJOSetter> pojoSetterList = new HashMap<>();
        for (Method method : SamplePOJOForSerialization.class.getDeclaredMethods()) {
            if (method.getName().startsWith("set")) {
                pojoSetterList.put(method.getName().substring(3).toLowerCase(),
                        SerializerUtils.compilePOJOSetter(method, ClickHouseColumn.of(method.getName(),
                                "String")));
            }
        }

        SamplePOJOForSerialization pojo = new SamplePOJOForSerialization();
        pojoSetterList.get("string").setValue(pojo, "John Doe");
        pojoSetterList.get("int32").setValue(pojo, Integer.valueOf(30));
        pojoSetterList.get("int16").setValue(pojo, 22);

        Assert.assertEquals(pojo.getString(), "John Doe");
        Assert.assertEquals(pojo.getInt32(), 30);
        Assert.assertEquals(pojo.getInt16(), 22);
    }

    public static class SamplePOJOInt256Setter implements POJOSetter {




        /*
          public void setValue(java.lang.Object, java.lang.Object);
            Code:
               0: aload_1
               1: checkcast     #7                  // class com/clickhouse/client/query/SamplePOJO
               4: aload_2
               5: checkcast     #25                 // class java/math/BigInteger
               8: invokevirtual #27                 // Method com/clickhouse/client/query/SamplePOJO.setInt256:(Ljava/math/BigInteger;)V
              11: return
         */
        @Override
        public void setValue(Object obj, Object value) {
            Arrays.stream(((Object[]) value)).collect(Collectors.toList());
        }
    }
}
