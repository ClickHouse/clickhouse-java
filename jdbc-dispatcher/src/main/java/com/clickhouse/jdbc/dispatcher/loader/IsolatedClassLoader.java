package com.clickhouse.jdbc.dispatcher.loader;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Driver;
import java.util.Enumeration;

/**
 * A classloader that loads JDBC driver classes in isolation from other driver versions.
 * This enables loading multiple versions of the same driver without class conflicts.
 * <p>
 * The classloader uses a child-first delegation model for driver-related classes,
 * while delegating to the parent for JDK and common library classes.
 */
public class IsolatedClassLoader extends URLClassLoader {

    private final String driverClassName;
    private final String version;

    /**
     * Creates a new IsolatedClassLoader for loading a driver JAR.
     *
     * @param jarUrl          the URL of the driver JAR file
     * @param parent          the parent classloader
     * @param driverClassName the fully qualified class name of the driver
     * @param version         the version identifier for this driver
     */
    public IsolatedClassLoader(URL jarUrl, ClassLoader parent, String driverClassName, String version) {
        super(new URL[]{jarUrl}, parent);
        this.driverClassName = driverClassName;
        this.version = version;
    }

    /**
     * Creates a new IsolatedClassLoader for loading multiple driver JARs.
     *
     * @param jarUrls         the URLs of the driver JAR files
     * @param parent          the parent classloader
     * @param driverClassName the fully qualified class name of the driver
     * @param version         the version identifier for this driver
     */
    public IsolatedClassLoader(URL[] jarUrls, ClassLoader parent, String driverClassName, String version) {
        super(jarUrls, parent);
        this.driverClassName = driverClassName;
        this.version = version;
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            // First, check if the class has already been loaded
            Class<?> loadedClass = findLoadedClass(name);
            if (loadedClass != null) {
                return loadedClass;
            }

            // For JDK classes and java.sql interfaces, delegate to parent
            if (shouldDelegateToParent(name)) {
                return super.loadClass(name, resolve);
            }

            // Try to find the class locally first (child-first for driver classes)
            try {
                loadedClass = findClass(name);
                if (resolve) {
                    resolveClass(loadedClass);
                }
                return loadedClass;
            } catch (ClassNotFoundException e) {
                // Fall back to parent classloader
                return super.loadClass(name, resolve);
            }
        }
    }

    /**
     * Determines whether class loading should be delegated to the parent classloader.
     *
     * @param className the fully qualified class name
     * @return true if the class should be loaded by the parent
     */
    private boolean shouldDelegateToParent(String className) {
        // Always delegate JDK classes
        if (className.startsWith("java.") ||
            className.startsWith("javax.") ||
            className.startsWith("jdk.") ||
            className.startsWith("sun.")) {
            return true;
        }

        // Delegate logging frameworks
        if (className.startsWith("org.slf4j.") ||
            className.startsWith("org.apache.log4j.") ||
            className.startsWith("ch.qos.logback.")) {
            return true;
        }

        return false;
    }

    @Override
    public URL getResource(String name) {
        // Child-first resource loading
        URL url = findResource(name);
        if (url != null) {
            return url;
        }
        return super.getResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        // Return resources from this classloader first, then parent
        Enumeration<URL> localResources = findResources(name);
        Enumeration<URL> parentResources = getParent().getResources(name);

        return new Enumeration<URL>() {
            @Override
            public boolean hasMoreElements() {
                return localResources.hasMoreElements() || parentResources.hasMoreElements();
            }

            @Override
            public URL nextElement() {
                if (localResources.hasMoreElements()) {
                    return localResources.nextElement();
                }
                return parentResources.nextElement();
            }
        };
    }

    /**
     * Loads and instantiates the JDBC driver from this classloader.
     *
     * @return the instantiated Driver
     * @throws ClassNotFoundException if the driver class cannot be found
     * @throws ReflectiveOperationException if the driver cannot be instantiated
     */
    public Driver loadDriver() throws ClassNotFoundException, ReflectiveOperationException {
        Class<?> driverClass = loadClass(driverClassName);
        if (!Driver.class.isAssignableFrom(driverClass)) {
            throw new ClassNotFoundException("Class " + driverClassName + " does not implement java.sql.Driver");
        }
        return (Driver) driverClass.getDeclaredConstructor().newInstance();
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "IsolatedClassLoader{" +
                "driverClassName='" + driverClassName + '\'' +
                ", version='" + version + '\'' +
                '}';
    }
}
