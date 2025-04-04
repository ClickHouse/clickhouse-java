CREATE TABLE big_columns_example
(
    -- Float64 columns
    float_col_1 Float64,
    float_col_2 Float64,
    float_col_3 Float64,
    float_col_4 Float64,
    float_col_5 Float64,
    float_col_6 Float64,
    float_col_7 Float64,
    float_col_8 Float64,
    float_col_9 Float64,
    float_col_10 Float64,

    -- String columns
    str_col_1 String,
    str_col_2 String,
    str_col_3 String,
    str_col_4 String,
    str_col_5 String,
    str_col_6 String,
    str_col_7 String,
    str_col_8 String,
    str_col_9 String,
    str_col_10 String,

    -- Int64 columns
    int_col_1 Int64,
    int_col_2 Int64,
    int_col_3 Int64,
    int_col_4 Int64,
    int_col_5 Int64,
    int_col_6 Int64,
    int_col_7 Int64,
    int_col_8 Int64,
    int_col_9 Int64,
    int_col_10 Int64
)
ENGINE = MergeTree
ORDER BY tuple();
