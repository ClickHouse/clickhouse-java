package com.clickhouse.client.http;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class Utils {

    @SuppressWarnings("unchecked")
    public static <T> T getInstance(Class<T> returnType, String className, Class<?>[] argTypes, Object[] args) {
        try {
            Class<?> clazz = Class.forName(className, false, Utils.class.getClassLoader());
            if (!returnType.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException(String.format("Invalid %s class type. Input class should be a superclass of %s.", className, returnType));
            }
            return getInstance(className, ((Class<T>) clazz).getConstructor(argTypes), argTypes, args);
        }
        catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(format("Class %s is not found in the classpath.", className));
        }
        catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(format("Class this does not have a constructor with %s argument type(s).", Arrays.stream(argTypes).collect(Collectors.toList())));
        }
    }

    public static <T> T getInstance(String className, Constructor<T> ctor, Class<?>[] argTypes, Object[] args) {
        try {
            return ctor.newInstance(args);
        }
        catch (SecurityException e) {
            throw new IllegalArgumentException((format("Error while creating an %s class instance. " +
                    "Number of constructor arguments or type of arguments differs. Expected arguments type: %s, given arguments: %s",
                    className, Arrays.toString(argTypes), Arrays.toString(args))));
        }
        catch (InvocationTargetException e) {
            throw new IllegalArgumentException(format("Error while creating an %s class instance. Constructor threw an exception.", className));
        }
        catch (IllegalAccessException e) {
            throw new IllegalArgumentException(format("Error while creating an %s class instance. Constructor is inaccessible.", className));
        }
        catch (InstantiationException e) {
            throw new IllegalArgumentException(format("Error while creating an %s class instance. Class is an abstract class.", className));
        }
    }
}
