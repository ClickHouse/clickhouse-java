package com.clickhouse.data;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Custom class loader for generated classes. It uses a thread-safe
 * {@link WeakHashMap} to maintain relationship between class name(String) and
 * class content(byte[]). The class will be only unloaded once there's no place
 * referring to the class name and GC is triggered.
 */
@Deprecated
public final class UnloadableClassLoader extends ClassLoader {
    public static final boolean HAS_ASM;

    static {
        Class<?> asmClass = null;
        try {
            asmClass = Class.forName("org.objectweb.asm.ClassWriter", true,
                    UnloadableClassLoader.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            // ignore
        }
        HAS_ASM = asmClass != null;
    }

    private final Map<String, Object> customClasses;
    private final ClassLoader parent;

    /**
     * Default constructor using {@link Thread#getContextClassLoader()} as parent.
     */
    public UnloadableClassLoader() {
        this(null);
    }

    /**
     * Constructs an unloadable class loader with specified parent class loader.
     *
     * @param parent optional class loader
     */
    public UnloadableClassLoader(ClassLoader parent) {
        this.customClasses = Collections.synchronizedMap(new WeakHashMap<>());
        if (parent != null) {
            this.parent = parent;
        } else {
            parent = Thread.currentThread().getContextClassLoader();
            if (parent == null) {
                parent = UnloadableClassLoader.class.getClassLoader();
            }
            this.parent = parent;
        }
    }

    /**
     * Adds a generated class.
     *
     * @param name  non-empty class name
     * @param bytes non-empty byte array representing the class
     */
    public void addClass(String name, byte[] bytes) {
        if (name == null || bytes == null || name.isEmpty() || bytes.length == 0) {
            throw new IllegalArgumentException("Non empty name and byte array are required");
        }
        customClasses.put(name, bytes);
    }

    /**
     * Explicitly unloads a generated class.
     *
     * @param name non-empty class name
     */
    public void unloadClass(String name) {
        customClasses.remove(name);
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        Object entry = customClasses.get(name);
        if (entry instanceof Class<?>) { // loaded class
            return (Class<?>) entry;
        } else if (entry instanceof byte[]) { // raw class
            byte[] bytes = (byte[]) entry;
            Class<?> clazz = defineClass(name, bytes, 0, bytes.length);
            customClasses.put(name, clazz);
            return clazz;
        }
        return super.findClass(name);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        if (customClasses.containsKey(name)) {
            return findClass(name);
        }
        return parent.loadClass(name);
    }
}