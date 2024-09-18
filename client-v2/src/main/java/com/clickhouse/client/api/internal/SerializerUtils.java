package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.insert.POJOSerializer;
import com.clickhouse.client.api.query.POJOSetter;
import com.clickhouse.data.ClickHouseAggregateFunction;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.data.value.ClickHouseBitmap;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.CHECKCAST;
import static org.objectweb.asm.Opcodes.INVOKEINTERFACE;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.RETURN;

public class SerializerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SerializerUtils.class);

    public static void serializeData(OutputStream stream, Object value, ClickHouseColumn column) throws IOException {
        //Serialize the value to the stream based on the data type
        switch (column.getDataType()) {
            case Array:
                serializeArrayData(stream, value, column);
                break;
            case Tuple:
                serializeTupleData(stream, value, column);
                break;
            case Map:
                serializeMapData(stream, value, column);
                break;
            case AggregateFunction:
                serializeAggregateFunction(stream, value, column);
                break;
            default:
                serializePrimitiveData(stream, value, column);
                break;

        }
    }

    private static void serializeArrayData(OutputStream stream, Object value, ClickHouseColumn column) throws IOException {
        //Serialize the array to the stream
        //The array is a list of values
        List<?> values = (List<?>) value;
        BinaryStreamUtils.writeVarInt(stream, values.size());
        for (Object val : values) {
            serializeData(stream, val, column.getArrayBaseColumn());
        }
    }

    private static void serializeTupleData(OutputStream stream, Object value, ClickHouseColumn column) throws IOException {
        //Serialize the tuple to the stream
        //The tuple is a list of values
        List<?> values = (List<?>) value;
        for (int i = 0; i < values.size(); i++) {
            serializeData(stream, values.get(i), column.getNestedColumns().get(i));
        }
    }

    private static void serializeMapData(OutputStream stream, Object value, ClickHouseColumn column) throws IOException {
        //Serialize the map to the stream
        //The map is a list of key-value pairs
        Map<?, ?> map = (Map<?, ?>) value;
        BinaryStreamUtils.writeVarInt(stream, map.size());
        map.forEach((key, val) -> {
            try {
                serializePrimitiveData(stream, key, Objects.requireNonNull(column.getKeyInfo()));
                serializeData(stream, val, Objects.requireNonNull(column.getValueInfo()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void serializePrimitiveData(OutputStream stream, Object value, ClickHouseColumn column) throws IOException {
        //Serialize the value to the stream based on the type
        switch (column.getDataType()) {
            case Int8:
                BinaryStreamUtils.writeInt8(stream, convertToInteger(value));
                break;
            case Int16:
                BinaryStreamUtils.writeInt16(stream, convertToInteger(value));
                break;
            case Int32:
                BinaryStreamUtils.writeInt32(stream, convertToInteger(value));
                break;
            case Int64:
                BinaryStreamUtils.writeInt64(stream, convertToLong(value));
                break;
            case Int128:
                BinaryStreamUtils.writeInt128(stream, convertToBigInteger(value));
                break;
            case Int256:
                BinaryStreamUtils.writeInt256(stream, convertToBigInteger(value));
                break;
            case UInt8:
                BinaryStreamUtils.writeUnsignedInt8(stream, convertToInteger(value));
                break;
            case UInt16:
                BinaryStreamUtils.writeUnsignedInt16(stream, convertToInteger(value));
                break;
            case UInt32:
                BinaryStreamUtils.writeUnsignedInt32(stream, convertToLong(value));
                break;
            case UInt64:
                BinaryStreamUtils.writeUnsignedInt64(stream, convertToLong(value));
                break;
            case UInt128:
                BinaryStreamUtils.writeUnsignedInt128(stream, convertToBigInteger(value));
                break;
            case UInt256:
                BinaryStreamUtils.writeUnsignedInt256(stream, convertToBigInteger(value));
                break;
            case Float32:
                BinaryStreamUtils.writeFloat32(stream, (Float) value);
                break;
            case Float64:
                BinaryStreamUtils.writeFloat64(stream, (Double) value);
                break;
            case Decimal:
            case Decimal32:
            case Decimal64:
            case Decimal128:
            case Decimal256:
                BinaryStreamUtils.writeDecimal(stream, (BigDecimal) value, column.getPrecision(), column.getScale());
                break;
            case Bool:
                BinaryStreamUtils.writeBoolean(stream, (Boolean) value);
                break;
            case String:
                BinaryStreamUtils.writeString(stream, (String) value);
                break;
            case FixedString:
                BinaryStreamUtils.writeFixedString(stream, (String) value, column.getPrecision());
                break;
            case Date:
                BinaryStreamUtils.writeDate(stream, (LocalDate) value);
                break;
            case Date32:
                BinaryStreamUtils.writeDate32(stream, (LocalDate) value);
                break;
            case DateTime: //TODO: Discuss LocalDateTime vs ZonedDateTime and time zones (Who uses LocalDateTime?)
                BinaryStreamUtils.writeDateTime(stream, (LocalDateTime) value, column.getTimeZone());
                break;
            case DateTime64:
                BinaryStreamUtils.writeDateTime64(stream, (LocalDateTime) value, column.getScale(), column.getTimeZone());
                break;
            case UUID:
                BinaryStreamUtils.writeUuid(stream, (UUID) value);
                break;
            case Enum8:
                BinaryStreamUtils.writeEnum8(stream, (Byte) value);
                break;
            case Enum16:
                BinaryStreamUtils.writeEnum16(stream, convertToInteger(value));
                break;
            case IPv4:
                BinaryStreamUtils.writeInet4Address(stream, (Inet4Address) value);
                break;
            case IPv6:
                BinaryStreamUtils.writeInet6Address(stream, (Inet6Address) value);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported data type: " + column.getDataType());
        }
    }

    private static void serializeAggregateFunction(OutputStream stream, Object value, ClickHouseColumn column) throws IOException {
        if (column.getAggregateFunction() == ClickHouseAggregateFunction.groupBitmap) {
            BinaryStreamUtils.writeBitmap(stream, (ClickHouseBitmap) value);
        } else {
            throw new UnsupportedOperationException("Unsupported aggregate function: " + column.getAggregateFunction());
        }
    }

    public static Integer convertToInteger(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        } else if (value instanceof Boolean) {
            return ((Boolean) value) ? 1 : 0;
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " to Integer");
        }
    }

    public static Long convertToLong(Object value) {
        if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof String) {
            return Long.parseLong((String) value);
        } else if (value instanceof Boolean) {
            return ((Boolean) value) ? 1L : 0L;
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " to Long");
        }
    }

    public static BigInteger convertToBigInteger(Object value) {
        if (value instanceof BigInteger) {
            return (BigInteger) value;
        } else if (value instanceof Number) {
            return BigInteger.valueOf(((Number) value).longValue());
        } else if (value instanceof String) {
            return new BigInteger((String) value);
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " to BigInteger");
        }
    }

    public static <T extends Enum<T>> Set<T> parseEnumList(String value, Class<T> enumType) {
        Set<T> values = new HashSet<>();
        for (StringTokenizer causes = new StringTokenizer(value, Client.VALUES_LIST_DELIMITER); causes.hasMoreTokens(); ) {
            values.add(Enum.valueOf(enumType, causes.nextToken()));
        }
        return values;
    }

    public static boolean convertToBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " to Boolean");
        }
    }

    public static List<?> convertArrayValueToList(Object value) {
        if (value instanceof BinaryStreamReader.ArrayValue) {
            return ((BinaryStreamReader.ArrayValue) value).asList();
        } else if (value.getClass().isArray()) {
            return  Arrays.stream(((Object[]) value)).collect(Collectors.toList());
        } else if (value instanceof List) {
            return (List<?>) value;
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " ('" + value.getClass() + "') to list");
        }
    }

    public static POJOSetter compilePOJOSetter(Method setterMethod, ClickHouseColumn column) {
        Class<?> dtoClass = setterMethod.getDeclaringClass();

        // creating a new class to implement POJOSetter which will call the setter method to set column value
        final String pojoSetterClassName = (dtoClass.getName() + setterMethod.getName()).replace('.', '/');
        ClassWriter writer = new ClassWriter(ClassWriter.COMPUTE_MAXS);
        writer.visit(Opcodes.V1_8, ACC_PUBLIC, pojoSetterClassName
                , null, "java/lang/Object",
                new String[]{POJOSetter.class.getName().replace('.', '/')});


        // constructor method
        {
            MethodVisitor mv = writer.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
            mv.visitVarInsn(ALOAD, 0);
            mv.visitMethodInsn(INVOKESPECIAL,
                    "java/lang/Object",
                    "<init>",
                    "()V");
            mv.visitInsn(RETURN);
            mv.visitMaxs(0, 0);
            mv.visitEnd();
        }

        /* Currently all readers operate with objects and next scenarios are possible:
            - target is primitive and source is a boxed type
                - source should be called `intValue()` or similar
            - target and source are both objects
                - no casting is needed
            - target is a boxed type and source is too, but smaller
                - source should be called `intValue()` or similar (target should be used to detect primitive type)
                - then target should be boxed with `valueOf()`
            - target is the assignable from source (e.g. target is `Object` and source is `String`)
                - no casting is needed
            - source should be converted before assigning to the target
                - call conversion function

            In the future when reader will use primitive types then call to `valueOf()` should be
            added for boxed types.
        */

        Class<?> targetType = setterMethod.getParameterTypes()[0];
        Class<?> targetPrimitiveType = ClickHouseDataType.toPrimitiveType(targetType); // will return object class if no primitive
        Class<?> sourceType = column.getDataType().getObjectClass(); // will return object class if no primitive

        // setter setValue(Object obj, Object value) impl
        {
            MethodVisitor mv = writer.visitMethod(ACC_PUBLIC, "setValue",
                    pojoSetterMethodDescriptor(Object.class, Object.class), null, null);

            mv.visitCode();
            mv.visitVarInsn(ALOAD, 1);
            mv.visitTypeInsn(CHECKCAST, Type.getInternalName(dtoClass));

            if (sourceType == LocalDate.class) {
                mv.visitVarInsn(ALOAD, 2); // load object
                mv.visitTypeInsn(CHECKCAST, Type.getInternalName(ZonedDateTime.class));
                mv.visitMethodInsn(INVOKEVIRTUAL,
                        Type.getInternalName(ZonedDateTime.class),
                        "toLocalDate",
                        "()" + Type.getDescriptor(LocalDate.class),
                        false);
            } else if (sourceType == LocalDateTime.class) {
                mv.visitVarInsn(ALOAD, 2); // load object
                mv.visitTypeInsn(CHECKCAST, Type.getInternalName(ZonedDateTime.class));
                mv.visitMethodInsn(INVOKEVIRTUAL,
                        Type.getInternalName(ZonedDateTime.class),
                        "toLocalDateTime",
                        "()" + Type.getDescriptor(LocalDateTime.class),
                        false);
            } else if ((targetType == boolean.class || targetType == Boolean.class) && column.getDataType() != ClickHouseDataType.Bool) {
                mv.visitVarInsn(ALOAD, 2); // load object
                String sourceInternalClassName;
                if (column.getDataType().isSigned()) {
                    sourceInternalClassName = Type.getInternalName(sourceType);
                } else if (column.getDataType() == ClickHouseDataType.UInt64) {
                    sourceInternalClassName = Type.getInternalName(BigInteger.class);
                } else {
                    sourceInternalClassName = Type.getInternalName(
                            ClickHouseDataType.toObjectType(ClickHouseDataType.toWiderPrimitiveType(
                                    ClickHouseDataType.toPrimitiveType(sourceType))));
                }
                mv.visitTypeInsn(CHECKCAST, sourceInternalClassName);
                mv.visitMethodInsn(INVOKESTATIC,
                        Type.getInternalName(SerializerUtils.class),
                        "convertToBoolean",
                        "(" + Type.getDescriptor(Object.class) + ")" + Type.getDescriptor(boolean.class),
                        false);
                if (!targetType.isPrimitive()) {
                    mv.visitMethodInsn(INVOKEVIRTUAL,
                            Type.getInternalName(Boolean.class),
                            "valueOf",
                            "(" + Type.getDescriptor(boolean.class) + ")" + Type.getDescriptor(targetType),
                            false);
                }
            } else if (column.getDataType() == ClickHouseDataType.Tuple && targetType.isAssignableFrom(List.class)) {
                mv.visitVarInsn(ALOAD, 2); // load object
                mv.visitTypeInsn(CHECKCAST, Type.getInternalName(Object[].class));
                mv.visitMethodInsn(INVOKESTATIC,
                        Type.getInternalName(Arrays.class),
                        "stream",
                        "([Ljava/lang/Object;)" + Type.getDescriptor(Stream.class),
                        false);
                mv.visitMethodInsn(INVOKESTATIC,
                        Type.getInternalName(Collectors.class),
                        "toList",
                        "()" + Type.getDescriptor(Collector.class),
                        false);
                mv.visitMethodInsn(INVOKEINTERFACE,
                        Type.getInternalName(Stream.class),
                        "collect",
                        "(" + Type.getDescriptor(Collector.class) + ")" + Type.getDescriptor(Object.class),
                        true);
            } else if (targetType.isAssignableFrom(sourceType)) { // assuming source is always object because of reader
                mv.visitVarInsn(ALOAD, 2); // load object
                mv.visitTypeInsn(CHECKCAST, Type.getInternalName(sourceType));
            } else if (column.getDataType() == ClickHouseDataType.Array) {
                mv.visitVarInsn(ALOAD, 2); // load object
                mv.visitTypeInsn(CHECKCAST, Type.getInternalName(BinaryStreamReader.ArrayValue.class));
                mv.visitMethodInsn(INVOKEVIRTUAL,
                        Type.getInternalName(BinaryStreamReader.ArrayValue.class),
                        "asList",
                        "()" + Type.getDescriptor(List.class),
                        false);
            } else if (targetType.isPrimitive() && !targetType.isArray()) {
                // unboxing
                mv.visitVarInsn(ALOAD, 2); // load object
                String sourceInternalClassName = getSourceInternalClassName(column, sourceType);
                mv.visitTypeInsn(CHECKCAST, sourceInternalClassName);
                mv.visitMethodInsn(INVOKEVIRTUAL,
                        sourceInternalClassName,
                        targetType.getSimpleName() + "Value",
                        "()" + Type.getDescriptor(targetType),
                        false);
            } else if (!targetPrimitiveType.isPrimitive()) {
                // boxing
                String sourceInternalClassName = getSourceInternalClassName(column, sourceType);
                mv.visitVarInsn(ALOAD, 2); // load object
                mv.visitTypeInsn(CHECKCAST, sourceInternalClassName);
                try {
                    if (!targetType.isAssignableFrom(Class.forName(sourceInternalClassName
                            .replaceAll("/", ".")))) {
                        mv.visitMethodInsn(INVOKEVIRTUAL,
                                sourceInternalClassName,
                                targetPrimitiveType.getSimpleName() + "Value",
                                "()" + Type.getDescriptor(targetPrimitiveType),
                                false);
                        mv.visitMethodInsn(INVOKESTATIC,
                                Type.getInternalName(targetType),
                                "valueOf",
                                "(" + Type.getDescriptor(targetPrimitiveType) + ")" + Type.getDescriptor(targetType),
                                false);
                    }
                } catch (ClassNotFoundException e) {
                    throw new ClientException("Cannot find class " + sourceInternalClassName + " to compile deserializer for "
                            + column.getColumnName(), e);
                }
            } else {
                throw new ClientException("Unsupported conversion from " + sourceType + " to " + targetType);
            }


            // finally call setter with the result of last INVOKEVIRTUAL
            mv.visitMethodInsn(INVOKEVIRTUAL,
                    Type.getInternalName(dtoClass),
                    setterMethod.getName(),
                    Type.getMethodDescriptor(setterMethod),
                    false);

            mv.visitInsn(RETURN);
            mv.visitMaxs(3, 3);
            mv.visitEnd();
        }

        try {
            SerializerUtils.DynamicClassLoader loader = new SerializerUtils.DynamicClassLoader(dtoClass.getClassLoader());
            Class<?> clazz = loader.defineClass(pojoSetterClassName.replace('/', '.'), writer.toByteArray());
            return (POJOSetter) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new ClientException("Failed to compile setter for " + setterMethod.getName(), e);
        }
    }

    private static String getSourceInternalClassName(ClickHouseColumn column, Class<?> sourceType) {
        String sourceInternalClassName;
        if (column.getDataType().isSigned()) {
            sourceInternalClassName = Type.getInternalName(sourceType);
        } else if (column.getDataType() == ClickHouseDataType.UInt64) {
            sourceInternalClassName = Type.getInternalName(BigInteger.class);
        } else if (column.getDataType() == ClickHouseDataType.Enum8) {
            sourceInternalClassName = Type.getInternalName(Byte.class);
        } else if (column.getDataType() == ClickHouseDataType.Enum16) {
            sourceInternalClassName = Type.getInternalName(Short.class);
        } else {
            sourceInternalClassName = Type.getInternalName(
                    ClickHouseDataType.toObjectType(ClickHouseDataType.toWiderPrimitiveType(
                            ClickHouseDataType.toPrimitiveType(sourceType))));
        }
        return sourceInternalClassName;
    }

    private static String pojoSetterMethodDescriptor(Class<?> dtoClass, Class<?> argType) {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        sb.append(Type.getDescriptor(dtoClass));
        sb.append(Type.getDescriptor(argType));
        sb.append(')');
        sb.append('V');
        return sb.toString();
    }

    public static class DynamicClassLoader extends ClassLoader {

        public DynamicClassLoader(ClassLoader classLoader) {
            super(classLoader);
        }
        public Class<?> defineClass(String name, byte[] code) throws ClassNotFoundException {
            return super.defineClass(name, code, 0, code.length);
        }
    }
}
