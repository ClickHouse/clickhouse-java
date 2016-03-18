package ru.yandex.metrika.clickhouse.util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;

/**
 * Created by jkee on 21.03.15.
 */
public class LogProxy<T> implements InvocationHandler {

    private static final Logger log = Logger.of(LogProxy.class);

    private final T object;
    private final Class<T> clazz;

    public static <T> T wrap(Class<T> interfaceClass, T object) {
        LogProxy<T> proxy = new LogProxy<T>(interfaceClass, object);
        return proxy.getProxy();
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
        log.info("Call class: " + clazz.getName() + " Method: " + method.getName() +
        " Args: " + Arrays.toString(args));
        try {
            return method.invoke(object, args);
        } catch (InvocationTargetException e) {
            throw e.getTargetException();
        }
    }
}
