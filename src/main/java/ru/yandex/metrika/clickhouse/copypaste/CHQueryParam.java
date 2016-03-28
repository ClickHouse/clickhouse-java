package ru.yandex.metrika.clickhouse.copypaste;

/**
 * @author serebrserg
 * @since 25.03.16
 */
public enum CHQueryParam {
    MAX_PARALLEL_REPLICAS,
    /**
     * Каким образом вычислять TOTALS при наличии HAVING, а также при наличии max_rows_to_group_by и group_by_overflow_mode = 'any'
     * https://clickhouse.yandex-team.ru/#%D0%9C%D0%BE%D0%B4%D0%B8%D1%84%D0%B8%D0%BA%D0%B0%D1%82%D0%BE%D1%80%20WITH%20TOTALS
     */
    TOTALS_MODE,
    /**
     * keyed - значит в параметре запроса передаётся "ключ" quota_key,
     и квота считается по отдельности для каждого значения ключа.
     Например, в качестве ключа может передаваться логин пользователя в Метрике,
     и тогда квота будет считаться для каждого логина по отдельности.
     Имеет смысл использовать только если quota_key передаётся не пользователем, а программой.
     */
    QUOTA_KEY,
    /**
     * Меньше значение - больше приоритет
     */
    PRIORITY,
    /**
     * БД по умолчанию.
     */
    DATABASE,
    /**
     *  сервер будет сжимать отправляемые вам данные
     */
    COMPRESS,
    /**
     * Вы можете получить в дополнение к результату также минимальные и максимальные значения по столбцам результата.
     * Для этого, выставите настройку extremes в 1. Минимумы и максимумы считаются для числовых типов, дат, дат-с-временем.
     * Для остальных столбцов, будут выведены значения по умолчанию.
     */
    EXTREMES,
    /**
     * Максимальное количество потоков обработки запроса
     * https://clickhouse.yandex-team.ru/#max_threads
     */
    MAX_THREADS,
    /**
     * Максимальное время выполнения запроса в секундах.
     * https://clickhouse.yandex-team.ru/#max_execution_time
     */
    MAX_EXECUTION_TIME,
    /**
     *  это рекомендация, какого размера блоки (в количестве строк) загружать из таблицы.
     * https://clickhouse.yandex-team.ru/#max_block_size
     */
    MAX_BLOCK_SIZE,
    /**
     * Профили настроек - это множество настроек, сгруппированных под одним именем.
     * Для каждого пользователя ClickHouse указывается некоторый профиль.
     */
    PROFILE,
    /**
     *  имя пользователя, по умолчанию - default.
     */
    USER;


    @Override
    public String toString() {
        return name().toLowerCase();
    }
}
