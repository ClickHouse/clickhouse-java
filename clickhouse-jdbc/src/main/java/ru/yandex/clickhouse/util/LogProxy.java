package ru.yandex.clickhouse.util;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;

import com.clickhouse.client.logging.Logger;
import com.clickhouse.client.logging.LoggerFactory;


public class LogProxy<T> implements InvocationHandler {

    private static final Logger log = LoggerFactory.getLogger(LogProxy.class);

    private final T object;
    private final Class<T> clazz;

    public static <T> T wrap(Class<T> interfaceClass, T object) {
        T[] wrapper = (T[]) Array.newInstance(interfaceClass, 1);
        wrapper[0] = object;
        log.trace(() -> {
            LogProxy<T> proxy = new LogProxy<T>(interfaceClass, object);
            wrapper[0] = proxy.getProxy();
            return "Proxy enabled for class: " + interfaceClass.getName();
        });

        return wrapper[0];
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
        String msg =
            "Call class: " + object.getClass().getName() +
            "\nMethod: " + method.getName() +
            "\nObject: " + object +
            "\nArgs: " + Arrays.toString(args) +
            "\nInvoke result: ";
        try {
            final Object invokeResult = method.invoke(object, args);
            msg +=  invokeResult;
            return invokeResult;
        } catch (InvocationTargetException e) {
            msg += e.getMessage();
            throw e.getTargetException();
        } finally {
            msg = "==== ClickHouse JDBC trace begin ====\n" + msg + "\n==== ClickHouse JDBC trace end ====";
            log.trace(msg);
        }
    }
}
