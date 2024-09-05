package com.clickhouse.client.api.internal;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.ClientException;
import com.clickhouse.client.api.data_formats.internal.BinaryStreamReader;
import com.clickhouse.client.api.query.POJOSetter;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.format.BinaryStreamUtils;
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
import java.util.stream.Collectors;

import static org.objectweb.asm.Opcodes.CHECKCAST;
import static org.objectweb.asm.Opcodes.DLOAD;
import static org.objectweb.asm.Opcodes.FLOAD;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.ILOAD;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.LLOAD;
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

    public static boolean getBooleanValue(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof Byte) {
            return (Byte) value != 0;
        } else if (value instanceof Short) {
            return (Short) value != 0;
        } else if (value instanceof Integer) {
            return (Integer) value != 0;
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " ('" + value.getClass() + "') to boolean");
        }
    }

    public static byte getByteValue(Object value) {
        if (value instanceof Byte) {
            return (Byte) value;
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " ('" + value.getClass() + "') to byte");
        }
    }

    public static LocalDateTime getLocalDateTimeValue(Object value) {
        if (value instanceof LocalDateTime) {
            return (LocalDateTime) value;
        } else if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).toLocalDateTime();
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " ('" + value.getClass() + "') to LocalDateTime");
        }
    }

    public static LocalDate getLocalDateValue(Object value) {
        if (value instanceof LocalDate) {
            return (LocalDate) value;
        } else if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).toLocalDate();
        } else if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).toLocalDate();
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " ('" + value.getClass() + "') to LocalDateTime");
        }
    }

    public static short getShortValue(Object value) {
        if (value instanceof Byte) {
            return (Byte) value;
        } else if (value instanceof Short) {
            return (Short) value;
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " ('" + value.getClass() + "') to short");
        }
    }

    public static int getIntValue(Object value) {
        if (value instanceof Byte) {
            return (Byte) value;
        } else if (value instanceof Short) {
            return (Short) value;
        } else if (value instanceof Integer) {
            return (Integer) value;
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " ('" + value.getClass() + "') to int");
        }
    }

    public static long getLongValue(Object value) {
        if (value instanceof Byte) {
            return (Byte) value;
        } else if (value instanceof Short) {
            return (Short) value;
        } else if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof BigInteger) {
            return ((BigInteger) value).longValueExact();
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " ('" + value.getClass() + "') to long");
        }
    }

    public static <T extends Enum<T>> Set<T> parseEnumList(String value, Class<T> enumType) {
        Set<T> values = new HashSet<>();
        for (StringTokenizer causes = new StringTokenizer(value, Client.VALUES_LIST_DELIMITER); causes.hasMoreTokens(); ) {
            values.add(Enum.valueOf(enumType, causes.nextToken()));
        }
        return values;
    }

    public static float getFloatValue(java.lang.Object value) {
        if (value instanceof Float) {
            return (Float) value;
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " ('" + value.getClass() + "') to float");
        }
    }

    public static double getDoubleValue(java.lang.Object value) {
        if (value instanceof Double) {
            return (Double) value;
        } else {
            throw new IllegalArgumentException("Cannot convert " + value + " ('" + value.getClass() + "') to double");
        }
    }

    public static List<?> getListValue(Object value) {
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

    public static POJOSetter<?> compilePOJOSetter(Method setterMethod) {
        Class<?> dtoClass = setterMethod.getDeclaringClass();
        Class<?> argType = setterMethod.getParameterTypes()[0];

        String deserializeMethod = null; // use default setter
        int typeLoadOperand = ALOAD; // any non-primitive
        if (argType.isPrimitive()) {
            typeLoadOperand = ILOAD; // // a boolean, byte, char, short, or int
            if (argType.getName().equalsIgnoreCase("boolean")) {
                deserializeMethod = "getBooleanValue";
            } else if (argType.getName().equalsIgnoreCase("byte")) {
                deserializeMethod = "getByteValue";
            } else if (argType.getName().equalsIgnoreCase("short")) {
                deserializeMethod = "getShortValue";
            } else if (argType.getName().equalsIgnoreCase("int")) {
                deserializeMethod = "getIntValue";
            } else if (argType.getName().equalsIgnoreCase("long")) {
                deserializeMethod = "getLongValue";
                typeLoadOperand = LLOAD;
            } else if (argType.getName().equalsIgnoreCase("float")) {
                deserializeMethod = "getFloatValue";
                typeLoadOperand = FLOAD;
            } else if (argType.getName().equalsIgnoreCase("double")) {
                deserializeMethod = "getDoubleValue";
                typeLoadOperand = DLOAD;
            } else {
                throw new IllegalArgumentException("Unsupported primitive type: " + argType.getName() + " " + argType);
            }
        } else if (argType.isAssignableFrom(LocalDateTime.class)) {
            deserializeMethod = "getLocalDateTimeValue";
        } else if (argType.isAssignableFrom(LocalDate.class)) {
            deserializeMethod = "getLocalDateValue";
        } else if (argType.isAssignableFrom(List.class)) {
            deserializeMethod = "getListValue";
        }

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

        if (deserializeMethod != null && argType.isPrimitive() ) {

            // primitive setter, ex setInt(int i)
            {
                MethodVisitor mv = writer.visitMethod(ACC_PUBLIC, "setValue", pojoSetterMethodDescriptor(dtoClass,
                        argType), null, null);

                mv.visitCode();
                mv.visitVarInsn(ALOAD, 1);
                mv.visitVarInsn(typeLoadOperand, 2);
                mv.visitMethodInsn(INVOKEVIRTUAL,
                        dtoClass.getName().replace('.', '/'),
                        setterMethod.getName(),
                        Type.getMethodDescriptor(setterMethod),
                        false);
                mv.visitInsn(RETURN);
                mv.visitMaxs(2, 2);
                mv.visitEnd();
            }

            // primitive setter setValue(Object obj, int value) impl (needed because generic types are not in runtime)
            {
                MethodVisitor mv = writer.visitMethod(ACC_PUBLIC, "setValue",
                        pojoSetterMethodDescriptor(Object.class, argType), null, null);

                mv.visitCode();
                mv.visitVarInsn(ALOAD, 0);
                mv.visitVarInsn(ALOAD, 1);
                mv.visitTypeInsn(CHECKCAST, Type.getInternalName(dtoClass));
                mv.visitVarInsn(typeLoadOperand, 2);
                mv.visitMethodInsn(INVOKEVIRTUAL,
                        pojoSetterClassName,
                        "setValue",
                        pojoSetterMethodDescriptor(dtoClass,
                                argType),
                        false);
                mv.visitInsn(RETURN);
                mv.visitMaxs(3, 3);
                mv.visitEnd();
            }
        }

        // main setter setValue(T obj, Object value) impl
        {
            MethodVisitor mv = writer.visitMethod(ACC_PUBLIC, "setValue", pojoSetterMethodDescriptor(dtoClass,
                    Object.class), null, null);

            mv.visitCode();
            mv.visitVarInsn(ALOAD, 1);
            mv.visitVarInsn(ALOAD, 2);
            if (deserializeMethod != null) {
                mv.visitMethodInsn(INVOKESTATIC,
                        "com/clickhouse/client/api/internal/SerializerUtils",
                        deserializeMethod,
                        "(Ljava/lang/Object;)" + Type.getDescriptor(argType),
                        false);
            } else {
                mv.visitTypeInsn(CHECKCAST, Type.getInternalName(argType));
            }
            mv.visitMethodInsn(INVOKEVIRTUAL,
                    dtoClass.getName().replace('.', '/'),
                    setterMethod.getName(),
                    Type.getMethodDescriptor(setterMethod),
                    false);
            mv.visitInsn(RETURN);
            mv.visitMaxs(2, 2);
            mv.visitEnd();
        }

        // main setter setValue(Object obj, Object value) impl (needed because generic types are not in runtime)
        {
            MethodVisitor mv = writer.visitMethod(ACC_PUBLIC, "setValue",
                    pojoSetterMethodDescriptor(Object.class, Object.class), null, null);

            mv.visitCode();
            mv.visitVarInsn(ALOAD, 0);
            mv.visitVarInsn(ALOAD, 1);
            mv.visitTypeInsn(CHECKCAST, Type.getInternalName(dtoClass));
            mv.visitVarInsn(ALOAD, 2);
            mv.visitMethodInsn(INVOKEVIRTUAL,
                    pojoSetterClassName,
                    "setValue",
                    pojoSetterMethodDescriptor(dtoClass,
                            Object.class),
                    false);
            mv.visitInsn(RETURN);
            mv.visitMaxs(3, 3);
            mv.visitEnd();
        }

        try {
            SerializerUtils.DynamicClassLoader loader = new SerializerUtils.DynamicClassLoader(dtoClass.getClassLoader());
            Class<?> clazz = loader.defineClass(pojoSetterClassName.replace('/', '.'), writer.toByteArray());
            return (POJOSetter<?>) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new ClientException("Failed to compile setter for " + setterMethod.getName(), e);
        }
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
