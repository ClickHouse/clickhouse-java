package com.clickhouse.client.datatypes;

import com.clickhouse.client.testdata.GenerateTestDataSet.DataSetEntry;
import com.clickhouse.client.testdata.TestDataSet;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates minimal POJO classes on the fly with ASM for entries in the test dataset catalog
 * ({@link com.clickhouse.client.testdata.GenerateTestDataSet#readDataSetIndex()}).
 *
 * <p>Each generated POJO has a public no-argument constructor, an {@code id} property ({@code int} by default)
 * and a {@code value} property typed according to the dataset's ClickHouse column type.
 */
public final class DataSetPojoGenerator {

    private static final String GENERATED_PACKAGE = "com.clickhouse.client.datatypes.generated.";
    private static final AtomicLong CLASS_SEQUENCE = new AtomicLong();
    private static final PojoClassLoader CLASS_LOADER =
            new PojoClassLoader(DataSetPojoGenerator.class.getClassLoader());

    private DataSetPojoGenerator() {
    }

    /**
     * Generates a POJO class with {@code int id} and a {@code value} property matching {@code entry}.
     */
    public static DataSetPojo forDataSet(DataSetEntry entry) {
        return forDataSet(entry, int.class, javaTypeFor(entry));
    }

    /**
     * Generates a POJO class with explicit {@code id} and {@code value} Java property types.
     */
    public static DataSetPojo forDataSet(DataSetEntry entry, Class<?> idType, Class<?> valueType) {
        Objects.requireNonNull(entry, "entry");
        Objects.requireNonNull(idType, "idType");
        Objects.requireNonNull(valueType, "valueType");

        Map<String, Class<?>> fields = new LinkedHashMap<>();
        fields.put(TestDataSet.ID_COLUMN, idType);
        fields.put("value", valueType);

        String className = GENERATED_PACKAGE + "DataSetPojo_"
                + sanitize(entry.getName()) + "_"
                + sanitize(valueType.getSimpleName()) + "_"
                + CLASS_SEQUENCE.incrementAndGet();
        Class<?> pojoClass = generateClass(className, fields);
        return new DataSetPojo(pojoClass, idType, valueType);
    }

    /**
     * Resolves the canonical Java property type for a dataset catalog entry, matching the Java
     * representation produced by {@link com.clickhouse.client.testdata.DataTypeGenerator}.
     */
    public static Class<?> javaTypeFor(DataSetEntry entry) {
        Objects.requireNonNull(entry, "entry");
        if ("BinaryString".equals(entry.getType())) {
            return byte[].class;
        }
        return javaTypeForColumn(ClickHouseColumn.of("value", entry.getClickHouseType()));
    }

    /**
     * Returns the primitive Java type for non-nullable entries whose canonical type is a boxed primitive,
     * or {@code null} when the entry cannot be represented as a Java primitive.
     */
    public static Class<?> primitiveJavaTypeFor(DataSetEntry entry) {
        Objects.requireNonNull(entry, "entry");
        ClickHouseColumn column = ClickHouseColumn.of("value", entry.getClickHouseType());
        if (column.isNullable()) {
            return null;
        }
        Class<?> boxed = javaTypeFor(entry);
        if (boxed == Byte.class) {
            return byte.class;
        } else if (boxed == Short.class) {
            return short.class;
        } else if (boxed == Integer.class) {
            return int.class;
        } else if (boxed == Long.class) {
            return long.class;
        } else if (boxed == Float.class) {
            return float.class;
        } else if (boxed == Double.class) {
            return double.class;
        } else if (boxed == Boolean.class) {
            return boolean.class;
        }
        return null;
    }

    static Class<?> javaTypeForColumn(ClickHouseColumn column) {
        ClickHouseDataType dataType = column.getDataType();
        switch (dataType) {
            case Int8:
                return Byte.class;
            case UInt8:
            case Int16:
                return Short.class;
            case UInt16:
            case Int32:
                return Integer.class;
            case UInt32:
            case Int64:
                return Long.class;
            case UInt64:
            case Int128:
            case UInt128:
            case Int256:
            case UInt256:
                return BigInteger.class;
            case Float32:
                return Float.class;
            case Float64:
                return Double.class;
            case Decimal:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal256:
                return BigDecimal.class;
            case Bool:
                return Boolean.class;
            case String:
            case FixedString:
            case Enum:
            case Enum8:
            case Enum16:
                return String.class;
            case Date:
            case Date32:
                return LocalDate.class;
            case Time:
            case Time64:
                return Duration.class;
            case DateTime:
            case DateTime32:
            case DateTime64:
                return ZonedDateTime.class;
            case IPv4:
                return Inet4Address.class;
            case IPv6:
                return Inet6Address.class;
            case UUID:
                return UUID.class;
            case Point:
                return double[].class;
            case Ring:
            case LineString:
            case MultiPoint:
                return double[][].class;
            case Polygon:
            case MultiLineString:
                return double[][][].class;
            case MultiPolygon:
                return double[][][][].class;
            case Array:
            case Tuple:
            case Nested:
                return List.class;
            case Map:
                return Map.class;
            default:
                throw new IllegalArgumentException(
                        "Unsupported dataset column type: " + column.getOriginalTypeName());
        }
    }

    /**
     * Generates and loads a public POJO class with a public no-arg constructor and JavaBean getters/setters
     * for every entry in {@code fields}.
     */
    public static Class<?> generateClass(String binaryClassName, Map<String, Class<?>> fields) {
        Objects.requireNonNull(binaryClassName, "binaryClassName");
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("At least one field is required");
        }

        String internalName = binaryClassName.replace('.', '/');
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
        cw.visit(Opcodes.V1_8, Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER, internalName,
                null, "java/lang/Object", null);

        // Public no-arg constructor
        MethodVisitor init = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
        init.visitCode();
        init.visitVarInsn(Opcodes.ALOAD, 0);
        init.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        init.visitInsn(Opcodes.RETURN);
        init.visitMaxs(1, 1);
        init.visitEnd();

        for (Map.Entry<String, Class<?>> entry : fields.entrySet()) {
            String fieldName = entry.getKey();
            Class<?> fieldType = entry.getValue();
            Type asmType = Type.getType(fieldType);
            String desc = asmType.getDescriptor();

            cw.visitField(Opcodes.ACC_PRIVATE, fieldName, desc, null, null).visitEnd();

            String suffix = Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);

            MethodVisitor getter = cw.visitMethod(Opcodes.ACC_PUBLIC, "get" + suffix, "()" + desc, null, null);
            getter.visitCode();
            getter.visitVarInsn(Opcodes.ALOAD, 0);
            getter.visitFieldInsn(Opcodes.GETFIELD, internalName, fieldName, desc);
            getter.visitInsn(asmType.getOpcode(Opcodes.IRETURN));
            getter.visitMaxs(0, 0);
            getter.visitEnd();

            MethodVisitor setter = cw.visitMethod(Opcodes.ACC_PUBLIC, "set" + suffix, "(" + desc + ")V", null, null);
            setter.visitCode();
            setter.visitVarInsn(Opcodes.ALOAD, 0);
            setter.visitVarInsn(asmType.getOpcode(Opcodes.ILOAD), 1);
            setter.visitFieldInsn(Opcodes.PUTFIELD, internalName, fieldName, desc);
            setter.visitInsn(Opcodes.RETURN);
            setter.visitMaxs(0, 0);
            setter.visitEnd();
        }

        cw.visitEnd();
        return CLASS_LOADER.defineClass(binaryClassName, cw.toByteArray());
    }

    private static String sanitize(String name) {
        String sanitized = name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_").replaceAll("^_|_$", "");
        return sanitized.isEmpty() ? "type" : sanitized;
    }

    /**
     * Handle for a dynamically generated {@code (id, value)} POJO class.
     */
    public static final class DataSetPojo {
        private final Class<?> pojoClass;
        private final Class<?> idType;
        private final Class<?> valueType;
        private final Constructor<?> constructor;
        private final Method getId;
        private final Method setId;
        private final Method getValue;
        private final Method setValue;

        DataSetPojo(Class<?> pojoClass, Class<?> idType, Class<?> valueType) {
            this.pojoClass = pojoClass;
            this.idType = idType;
            this.valueType = valueType;
            try {
                this.constructor = pojoClass.getDeclaredConstructor();
                this.getId = pojoClass.getMethod("getId");
                this.setId = pojoClass.getMethod("setId", idType);
                this.getValue = pojoClass.getMethod("getValue");
                this.setValue = pojoClass.getMethod("setValue", valueType);
            } catch (NoSuchMethodException e) {
                throw new IllegalStateException("Generated POJO is missing expected accessor methods", e);
            }
        }

        public Class<?> getPojoClass() {
            return pojoClass;
        }

        public Class<?> getIdType() {
            return idType;
        }

        public Class<?> getValueType() {
            return valueType;
        }

        public Object newInstance(int id, Object value) {
            try {
                Object instance = constructor.newInstance();
                setId.invoke(instance, id);
                setValue.invoke(instance, value);
                return instance;
            } catch (Exception e) {
                throw new IllegalStateException("Failed to instantiate/populate " + pojoClass.getName(), e);
            }
        }

        public List<Object> toPojoList(TestDataSet dataSet) {
            List<Object[]> rows = dataSet.getRows();
            List<Object> pojos = new ArrayList<>(rows.size());
            for (Object[] row : rows) {
                pojos.add(newInstance((Integer) row[0], row[1]));
            }
            return Collections.unmodifiableList(pojos);
        }

        public int getId(Object pojo) {
            try {
                return ((Number) getId.invoke(pojo)).intValue();
            } catch (Exception e) {
                throw new IllegalStateException("Failed to read id from " + pojoClass.getName(), e);
            }
        }

        public Object getValue(Object pojo) {
            try {
                return getValue.invoke(pojo);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to read value from " + pojoClass.getName(), e);
            }
        }
    }

    private static final class PojoClassLoader extends ClassLoader {
        PojoClassLoader(ClassLoader parent) {
            super(parent);
        }

        Class<?> defineClass(String name, byte[] bytecode) {
            return defineClass(name, bytecode, 0, bytecode.length);
        }
    }
}
