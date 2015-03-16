package ru.yandex.metrika.clickhouse.util;

import java.sql.DriverManager;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;

/**
 * Тупой логгер. При желании можно перейти на что-то приличное, что умеет писать в PrintWriter и конфигуриться на лету.
 *
 * Created by jkee on 16.03.15.
 */
public class Logger {

    private static Level currentLevel = Level.INFO; // todo configuration

    private final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final String key;

    private Logger(String key) {
        this.key = key;
    }

    public static Logger logger(String key) {
        return new Logger(key);
    }

    public static Logger of(Class clazz) {
        return new Logger(clazz.getSimpleName());
    }

    public static Logger of(Object object) {
        return new Logger(object.getClass().getSimpleName());
    }

    public void info(String message) {
        log(message, Level.INFO);
    }

    public void debug(String message) {
        log(message, Level.FINE);
    }

    public void warn(String message) {
        log(message, Level.WARNING);
    }

    public void error(String message) {
        log(message, Level.SEVERE);
    }

    public synchronized void log(String message, Level level) {
        if (level.intValue() >= currentLevel.intValue()) {
            String str = String.format("%s <%s> %s: %s", DATE_FORMAT.format(new Date()), level.getName(), key, message);
            DriverManager.println(str);
        }
    }

}
