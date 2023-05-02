package com.clickhouse.data;

import java.util.UUID;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.testng.Assert;
import org.testng.annotations.Test;

public class UnloadableClassLoaderTest {
    public static class MyPojo {
        private String name;

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return this.name;
        }
    }

    private byte[] generateTestClass(String className) {
        String pojoClassName = MyPojo.class.getName().replace('.', '/');
        ClassWriter cw = new ClassWriter(0);
        cw.visit(Opcodes.V1_8, Opcodes.ACC_PUBLIC, className.replace('.', '/'), null, "java/lang/Object",
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
        mv.visitTypeInsn(Opcodes.CHECKCAST, pojoClassName);
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
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, pojoClassName, "setName", "(Ljava/lang/String;)V", false);

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

    @Test(groups = { "unit" })
    public void testAddInvalidClass() throws Exception {
        UnloadableClassLoader cl = new UnloadableClassLoader();
        Assert.assertThrows(IllegalArgumentException.class, () -> cl.addClass(null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> cl.addClass(null, new byte[0]));
        Assert.assertThrows(IllegalArgumentException.class, () -> cl.addClass("", null));
        Assert.assertThrows(IllegalArgumentException.class, () -> cl.addClass("", new byte[0]));
    }

    @Test(groups = { "unit" })
    public void testLoadAndUnload() throws Exception {
        UnloadableClassLoader cl = new UnloadableClassLoader();

        String className = "unknown." + UUID.randomUUID().toString().replace('-', 'x');
        Assert.assertThrows(ClassNotFoundException.class, () -> cl.loadClass(className));

        cl.addClass(className, generateTestClass(className));
        Assert.assertNotNull(cl.loadClass(className));
        Assert.assertTrue(cl.loadClass(className) == cl.loadClass(className));

        cl.unloadClass(className);
        Assert.assertThrows(ClassNotFoundException.class, () -> cl.loadClass(className));

        // reload is not supported
        cl.addClass(className, generateTestClass(className));
        Assert.assertThrows(LinkageError.class, () -> cl.loadClass(className));
    }

    // @Test(groups = { "unit" })
    public void testAutoUnload() throws Exception {
        UnloadableClassLoader cl = new UnloadableClassLoader();
        String uuid = UUID.randomUUID().toString().replace('-', 'x');
        String className = "unknown." + uuid;
        cl.addClass(className, generateTestClass(className));
        Assert.assertNotNull(cl.loadClass(className));

        className = null;
        System.gc();
        Thread.sleep(10000);
        Assert.assertThrows(ClassNotFoundException.class, () -> cl.loadClass("unknown." + uuid));
    }
}