package com.clickhouse.client.config;

import java.io.Serializable;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.clickhouse.client.ClickHouseChecker;
import com.clickhouse.client.ClickHouseDataType;
import com.clickhouse.client.ClickHouseFormat;

public class ClickHouseConfigOptionTest {
        static enum ClickHouseTestOption implements ClickHouseOption {
                STR("string_option", "string", "string option"),
                STR0("string_option0", "string0", "string option without environment variable support"),
                STR1("string_option0", "string1",
                                "string option without environment variable and system property support"),
                INT("integer_option", 2333, "integer option"),
                INT0("integer_option0", 23330, "integer option without environment variable support"),
                INT1("integer_option1", 23331,
                                "integer option without environment variable and system property support"),
                BOOL("boolean_option", false, "boolean option"),
                BOOL0("boolean_option0", true, "boolean option without environment variable support"),
                BOOL1("boolean_option1", false,
                                "boolean option without environment variable and system property support");

                private final String key;
                private final Serializable defaultValue;
                private final Class<? extends Serializable> clazz;
                private final String description;

                <T extends Serializable> ClickHouseTestOption(String key, T defaultValue, String description) {
                        this.key = ClickHouseChecker.nonNull(key, "key");
                        this.defaultValue = defaultValue;
                        this.clazz = defaultValue.getClass();
                        this.description = ClickHouseChecker.nonNull(description, "description");
                }

                @Override
                public String getKey() {
                        return key;
                }

                @Override
                public Serializable getDefaultValue() {
                        return defaultValue;
                }

                @Override
                public Class<? extends Serializable> getValueType() {
                        return clazz;
                }

                @Override
                public String getDescription() {
                        return description;
                }
        }

        @Test(groups = { "unit" })
        public void testFromString() {
                Assert.assertThrows(IllegalArgumentException.class,
                                () -> ClickHouseOption.fromString(null, String.class));
                Assert.assertEquals(ClickHouseOption.fromString("", String.class), "");

                Assert.assertEquals(ClickHouseOption.fromString("", Boolean.class), Boolean.FALSE);
                Assert.assertEquals(ClickHouseOption.fromString("Yes", Boolean.class), Boolean.FALSE);
                Assert.assertEquals(ClickHouseOption.fromString("1", Boolean.class), Boolean.TRUE);
                Assert.assertEquals(ClickHouseOption.fromString("true", Boolean.class), Boolean.TRUE);
                Assert.assertEquals(ClickHouseOption.fromString("True", Boolean.class), Boolean.TRUE);

                Assert.assertEquals(ClickHouseOption.fromString("", Integer.class), Integer.valueOf(0));
                Assert.assertEquals(ClickHouseOption.fromString("0", Integer.class), Integer.valueOf(0));
                Assert.assertThrows(IllegalArgumentException.class,
                                () -> ClickHouseOption.fromString(null, Integer.class));

                Assert.assertEquals(ClickHouseOption.fromString("0.1", Float.class), Float.valueOf(0.1F));
                Assert.assertEquals(ClickHouseOption.fromString("NaN", Float.class), Float.valueOf(Float.NaN));

                Assert.assertEquals(ClickHouseOption.fromString("Map", ClickHouseDataType.class),
                                ClickHouseDataType.Map);
                Assert.assertEquals(ClickHouseOption.fromString("RowBinary", ClickHouseFormat.class),
                                ClickHouseFormat.RowBinary);
                Assert.assertThrows(IllegalArgumentException.class,
                                () -> ClickHouseOption.fromString("NonExistFormat", ClickHouseFormat.class));
        }

        @Test(groups = { "unit" })
        public void testGetEffectiveDefaultValue() {
                // environment variables are set in pom.xml
                Assert.assertEquals(ClickHouseTestOption.STR.getEffectiveDefaultValue(),
                                ClickHouseTestOption.STR.getDefaultValueFromEnvVar().get());
                Assert.assertEquals(ClickHouseTestOption.INT.getEffectiveDefaultValue(),
                                Integer.parseInt(ClickHouseTestOption.INT.getDefaultValueFromEnvVar().get()));
                Assert.assertEquals(ClickHouseTestOption.BOOL.getEffectiveDefaultValue(),
                                Boolean.valueOf(ClickHouseTestOption.BOOL.getDefaultValueFromEnvVar().get()));

                String sv = "system.property";
                int iv = 12345;
                boolean bv = true;
                System.setProperty(ClickHouseTestOption.STR0.getPrefix().toLowerCase() + "_"
                                + ClickHouseTestOption.STR0.name().toLowerCase(), sv);
                System.setProperty(ClickHouseTestOption.INT0.getPrefix().toLowerCase() + "_"
                                + ClickHouseTestOption.INT0.name().toLowerCase(), String.valueOf(iv));
                System.setProperty(ClickHouseTestOption.BOOL0.getPrefix().toLowerCase() + "_"
                                + ClickHouseTestOption.BOOL0.name().toLowerCase(), String.valueOf(bv));

                Assert.assertEquals(ClickHouseTestOption.STR0.getEffectiveDefaultValue(), sv);
                Assert.assertEquals(ClickHouseTestOption.INT0.getEffectiveDefaultValue(), iv);
                Assert.assertEquals(ClickHouseTestOption.BOOL0.getEffectiveDefaultValue(), bv);

                Assert.assertEquals(ClickHouseTestOption.STR1.getEffectiveDefaultValue(),
                                ClickHouseTestOption.STR1.getDefaultValue());
                Assert.assertEquals(ClickHouseTestOption.INT1.getEffectiveDefaultValue(),
                                ClickHouseTestOption.INT1.getDefaultValue());
                Assert.assertEquals(ClickHouseTestOption.BOOL1.getEffectiveDefaultValue(),
                                ClickHouseTestOption.BOOL1.getDefaultValue());
        }
}
