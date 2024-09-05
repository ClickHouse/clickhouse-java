package com.clickhouse.client.internal;

import com.clickhouse.client.api.internal.SerializerUtils;
import com.clickhouse.client.api.query.POJOSetter;
import com.clickhouse.client.query.SamplePOJO;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public class SerializerUtilsTests {


    @Test
    public void testDeserialize() throws Exception {

        Map<String, POJOSetter> pojoSetterList = new HashMap<>();
        for (Method method : SamplePOJOForSerialization.class.getDeclaredMethods()) {
            if (method.getName().startsWith("set")) {
                pojoSetterList.put(method.getName().substring(3).toLowerCase(),
                        SerializerUtils.compilePOJOSetter(method));
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

    public static class SamplePOJOInt256Setter implements POJOSetter<SamplePOJO> {

        @Override
        public void setValue(SamplePOJO obj, int value) {
            obj.setInt16(value);
        }

        @Override
        public void setValue(SamplePOJO obj, Object value) {
            obj.setInt256((BigInteger) value);
        }
    }
}
