package com.clickhouse.client;

import java.util.Optional;
import java.util.ServiceLoader;

import javax.net.ssl.SSLException;

import com.clickhouse.data.ClickHouseChecker;

/**
 * This interface defines how to build SSL context based on given configuration
 * and target server.
 */
public interface ClickHouseSslContextProvider {
    /**
     * Get non-null SSL context provider.
     * 
     * @return non-null SSL context provider
     */
    static ClickHouseSslContextProvider getProvider() {
        String packageName = ClickHouseSslContextProvider.class.getName();
        packageName = packageName.substring(0, packageName.lastIndexOf('.') + 1);
        ClickHouseSslContextProvider defaultProvider = null;
        for (ClickHouseSslContextProvider s : ServiceLoader.load(ClickHouseSslContextProvider.class,
                ClickHouseSslContextProvider.class.getClassLoader())) {
            if (s == null) {
                // impossible
            } else if (s.getClass().getName().startsWith(packageName)) {
                defaultProvider = s;
            } else {
                return s;
            }
        }
        return ClickHouseChecker.nonNull(defaultProvider, ClickHouseSslContextProvider.class.getSimpleName());
    }

    /**
     * Get SSL context.
     * 
     * @param <T>             type of SSL context
     * @param sslContextClass SSL context class
     * @param config          client config
     * @return SSL context
     * @throws SSLException when error occured getting SSL context
     */
    <T> Optional<T> getSslContext(Class<? extends T> sslContextClass, ClickHouseConfig config) throws SSLException;
}
