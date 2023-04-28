package com.clickhouse.data.mapper;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseRecord;
import com.clickhouse.data.ClickHouseRecordMapper;
import com.clickhouse.data.ClickHouseValue;

public class CompiledRecordMapper extends AbstractRecordMapper {
    static final class PropertyDescriptor {
        int valueIndex;
        String name;
        String argType;
        String returnType;
        String colDefinition;
    }

    private static byte[] generateMapperClass(Class<?> objClass) {
        String className = objClass.getName().replace('.', '/');
        ClassWriter cw = new ClassWriter(0);
        cw.visit(Opcodes.V1_8, Opcodes.ACC_PUBLIC, className, null, "java/lang/Object",
                new String[] { ClickHouseRecordMapper.class.getName().replace('.', '/') });

        MethodVisitor mv;

        // Constructor
        mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(1, 1);
        mv.visitEnd();

        // mapTo method
        mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "mapTo",
                "(Lcom/clickhouse/data/ClickHouseRecord;Ljava/lang/Class;Ljava/lang/Object;)Ljava/lang/Object;",
                "<T:Ljava/lang/Object;>(Lcom/clickhouse/data/ClickHouseRecord;Ljava/lang/Class<TT;>;TT;)TT;", null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 3);
        mv.visitTypeInsn(Opcodes.CHECKCAST, "com/mycompany/Pojo");
        mv.visitVarInsn(Opcodes.ASTORE, 4);
        mv.visitInsn(Opcodes.ICONST_0);
        mv.visitVarInsn(Opcodes.ISTORE, 5);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "com/clickhouse/data/ClickHouseRecord", "size", "()I", true);
        mv.visitVarInsn(Opcodes.ISTORE, 6);

        // for loop
        mv.visitInsn(Opcodes.ICONST_0);
        mv.visitVarInsn(Opcodes.ISTORE, 7);
        mv.visitLabel(new Label());
        mv.visitVarInsn(Opcodes.ILOAD, 7);
        mv.visitVarInsn(Opcodes.ILOAD, 6);
        mv.visitJumpInsn(Opcodes.IF_ICMPGE, new Label());

        mv.visitVarInsn(Opcodes.ALOAD, 4);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitVarInsn(Opcodes.ILOAD, 7);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "com/clickhouse/data/ClickHouseRecord", "getValue",
                "(I)LValueObject;", true);
        mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, "ValueObject", "asString", "()Ljava/lang/String;", true);
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "com/mycompany/Pojo", "setName", "(Ljava/lang/String;)V", false);

        mv.visitIincInsn(7, 1);
        mv.visitJumpInsn(Opcodes.GOTO, new Label());

        // return statement
        mv.visitLabel(new Label());
        mv.visitVarInsn(Opcodes.ALOAD, 4);
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitMaxs(3, 8);
        mv.visitEnd();

        return cw.toByteArray();
    }

    private final Constructor<?> constructor;
    private final PropertyDescriptor[] descriptors;

    private CompiledRecordMapper(Class<?> objClass, Constructor<?> constructor, List<ClickHouseColumn> columns) {
        super(objClass);
        this.constructor = constructor;

        if (columns == null || columns.isEmpty()) {
            this.descriptors = new PropertyDescriptor[0];
        } else {
            int size = columns.size();
            List<PropertyDescriptor> list = new ArrayList<>(size);
            Method[] setters = getSetterMethods(objClass, columns);
            for (int i = 0; i < size; i++) {
                Method m = setters[i];
                if (m != null) {
                    list.add(new PropertyDescriptor());
                }
            }
            this.descriptors = list.toArray(new PropertyDescriptor[0]);
        }
    }

    protected CompiledRecordMapper(Class<?> objClass) {
        this(objClass, getDefaultConstructor(objClass), Collections.emptyList());
    }

    @Override
    public ClickHouseRecordMapper get(List<ClickHouseColumn> columns) {
        return new CompiledRecordMapper(clazz, constructor, columns);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T mapTo(ClickHouseRecord r, Class<T> objClass, T obj) {
        check(r, objClass);

        if (obj == null) {
            obj = (T) newInstance(constructor);
        }

        for (int i = 0, len = descriptors.length; i < len; i++) {
            PropertyDescriptor d = descriptors[i];
            ClickHouseValue v = r.getValue(d.valueIndex);
        }
        return obj;
    }
}