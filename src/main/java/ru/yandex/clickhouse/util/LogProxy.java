package ru.yandex.clickhouse.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;


public class LogProxy<T> implements InvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(LogProxy.class);

    private final T object;
    private final Class<T> clazz;

    public static <T> T wrap(Class<T> interfaceClass, T object) {
        if (log.isTraceEnabled()) {
            LogProxy<T> proxy = new LogProxy<T>(interfaceClass, object);
            return proxy.getProxy();
        }
        return object;
    }

    private LogProxy(Class<T> interfaceClass, T object) {
        if (!interfaceClass.isInterface()) {
            throw new IllegalStateException("Class " + interfaceClass.getName() + " is not an interface");
        }
        clazz = interfaceClass;
        this.object = object;
    }

    @SuppressWarnings("unchecked")
    public T getProxy() {
        //xnoinspection x
        // unchecked
        return (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class<?>[]{clazz}, this);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        log.trace(
            "Call class: " + clazz.getName() + " Method: " + method.getName() + " Args: " + Arrays.toString(args)
        );
        try {
            return method.invoke(object, args);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }
}
