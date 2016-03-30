package ru.yandex.metrika.clickhouse.copypaste;

/**
 * @author serebrserg
 * @since 25.03.16
 */
public enum CHQueryParam {
    MAX_PARALLEL_REPLICAS("max_parallel_replicas", null, Integer.class),
    /**
     * Каким образом вычислять TOTALS при наличии HAVING, а также при наличии max_rows_to_group_by и group_by_overflow_mode = 'any'
     * https://clickhouse.yandex-team.ru/#%D0%9C%D0%BE%D0%B4%D0%B8%D1%84%D0%B8%D0%BA%D0%B0%D1%82%D0%BE%D1%80%20WITH%20TOTALS
     */
    TOTALS_MODE("totals_mode", null, String.class),
    /**
     * keyed - значит в параметре запроса передаётся "ключ" quota_key,
     и квота считается по отдельности для каждого значения ключа.
     Например, в качестве ключа может передаваться логин пользователя в Метрике,
     и тогда квота будет считаться для каждого логина по отдельности.
     Имеет смысл использовать только если quota_key передаётся не пользователем, а программой.
     */
    QUOTA_KEY("quota_key", null, String.class),
    /**
     * Меньше значение - больше приоритет
     */
    PRIORITY("priority", null, Integer.class),
    /**
     * БД по умолчанию.
     */
    DATABASE("database", null, String.class),
    /**
     *  сервер будет сжимать отправляемые вам данные
     */
    COMPRESS("compress", true, Boolean.class),
    /**
     * Вы можете получить в дополнение к результату также минимальные и максимальные значения по столбцам результата.
     * Для этого, выставите настройку extremes в 1. Минимумы и максимумы считаются для числовых типов, дат, дат-с-временем.
     * Для остальных столбцов, будут выведены значения по умолчанию.
     */
    EXTREMES("extremes", false, String.class),
    /**
     * Максимальное количество потоков обработки запроса
     * https://clickhouse.yandex-team.ru/#max_threads
     */
    MAX_THREADS("max_threads", null, Integer.class),
    /**
     * Максимальное время выполнения запроса в секундах.
     * https://clickhouse.yandex-team.ru/#max_execution_time
     */
    MAX_EXECUTION_TIME("max_execution_time", null, Integer.class),
    /**
     *  это рекомендация, какого размера блоки (в количестве строк) загружать из таблицы.
     * https://clickhouse.yandex-team.ru/#max_block_size
     */
    MAX_BLOCK_SIZE("max_block_size", null, Integer.class),
    /**
     * Профили настроек - это множество настроек, сгруппированных под одним именем.
     * Для каждого пользователя ClickHouse указывается некоторый профиль.
     */
    PROFILE("profile", null, String.class),
    /**
     *  имя пользователя, по умолчанию - default.
     */
    USER("user", null, String.class);

    private final String key;
    private final Object defaultValue;
    private final Class clazz;

    CHQueryParam(String key, Object defaultValue, Class clazz) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.clazz = clazz;
    }

    public String getKey() {
        return key;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public Class getClazz() {
        return clazz;
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}
