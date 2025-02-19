package com.clickhouse.data;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Deprecated
public enum ClickHouseAggregateFunction {
    // select concat(f.name, '(', f.case_insensitive ? 'true' : 'false',
    // a.names != '' ? concat(',', replaceAll(replaceRegexpAll(a.names,
    // '^\\[(.*)\\]$', '\\1'), '''', '"')) : '', '),') as x
    // from system.functions f
    // left outer join (
    // select alias_to, toString(groupArray(case_insensitive ? upper(name) : name))
    // as names
    // from system.functions
    // where is_aggregate != 0 and alias_to != ''
    // group by alias_to
    // ) a on f.name = a.alias_to
    // where f.is_aggregate != 0 and f.alias_to = ''
    // order by f.name
    aggThrow(false, 0, true, 0, ClickHouseDataType.Nothing),
    any(false, 1, true, 1, null),
    anyHeavy(false, 1, false, 1, null),
    anyLast(false, 1, true, 1, null),
    argMax(false, 2, false, 1, null),
    argMin(false, 2, false, 1, null),
    avg(true, 1, false, 1, null),
    avgWeighted(false, 2, false, 1, null),
    boundingRatio(false, 2, false, 0, ClickHouseDataType.Float64),
    categoricalInformationValue(false, Integer.MAX_VALUE, false, 0, ClickHouseDataType.Float64),
    corr(true, 2, false, 0, ClickHouseDataType.Float64),
    corrStable(false, 2, false, 0, ClickHouseDataType.Float64),
    count(true, 1, true, 0, ClickHouseDataType.UInt64),
    covarPop(false, 2, false, 0, ClickHouseDataType.Float64, "COVAR_POP"),
    covarPopStable(false, 2, false, 0, ClickHouseDataType.Float64),
    covarSamp(false, 2, false, 0, ClickHouseDataType.Float64, "COVAR_SAMP"),
    covarSampStable(false, 2, false, 0, ClickHouseDataType.Float64),
    deltaSum(false, 1, false, 1, null),
    deltaSumTimestamp(false, 2, false, 1, null),
    dense_rank(true),
    entropy(false, 1, false, 0, ClickHouseDataType.Float64),
    exponentialMovingAverage(false, 3, false, 0, ClickHouseDataType.Float64),
    exponentialTimeDecayedAvg(false),
    exponentialTimeDecayedCount(false),
    exponentialTimeDecayedMax(false),
    exponentialTimeDecayedSum(false),
    first_value(true, 1, true, 1, null),
    groupArray(false, 2, false, 1, null),
    groupArrayInsertAt(false),
    groupArrayMovingAvg(false),
    groupArrayMovingSum(false),
    groupArraySample(false),
    groupBitAnd(false, "BIT_AND"),
    groupBitOr(false, "BIT_OR"),
    groupBitXor(false, "BIT_XOR"),
    groupBitmap(false),
    groupBitmapAnd(false),
    groupBitmapOr(false),
    groupBitmapXor(false),
    groupUniqArray(false),
    histogram(false),
    intervalLengthSum(false),
    kurtPop(false),
    kurtSamp(false),
    lagInFrame(false),
    last_value(true, 1, true, 1, null),
    leadInFrame(false),
    mannWhitneyUTest(false),
    max(true, 1, true, 1, null),
    maxIntersections(false),
    maxIntersectionsPosition(false),
    maxMappedArrays(false),
    min(true, 1, true, 1, null),
    minMappedArrays(false),
    quantile(false, 2, false, 0, ClickHouseDataType.Float64, "median"),
    quantileBFloat16(false, 2, false, 0, ClickHouseDataType.Float64,
            "medianBFloat16"),
    quantileBFloat16Weighted(false, "medianBFloat16Weighted"),
    quantileDeterministic(false, "medianDeterministic"),
    quantileExact(false, "medianExact"),
    quantileExactExclusive(false),
    quantileExactHigh(false, "medianExactHigh"),
    quantileExactInclusive(false),
    quantileExactLow(false, "medianExactLow"),
    quantileExactWeighted(false, "medianExactWeighted"),
    quantileTDigest(false, "medianTDigest"),
    quantileTDigestWeighted(false, "medianTDigestWeighted"),
    quantileTiming(false, "medianTiming"),
    quantileTimingWeighted(false, "medianTimingWeighted"),
    quantiles(false, Integer.MAX_VALUE, false, 0, ClickHouseDataType.Float64),
    quantilesBFloat16(false),
    quantilesBFloat16Weighted(false),
    quantilesDeterministic(false),
    quantilesExact(false),
    quantilesExactExclusive(false),
    quantilesExactHigh(false),
    quantilesExactInclusive(false),
    quantilesExactLow(false),
    quantilesExactWeighted(false),
    quantilesTDigest(false),
    quantilesTDigestWeighted(false),
    quantilesTiming(false),
    quantilesTimingWeighted(false),
    rank(true),
    rankCorr(false),
    retention(false),
    row_number(true),
    sequenceCount(false),
    sequenceMatch(false),
    sequenceNextNode(false),
    simpleLinearRegression(false),
    singleValueOrNull(false),
    skewPop(false),
    skewSamp(false),
    sparkbar(false),
    stddevPop(false, "STDDEV_POP"),
    stddevPopStable(false),
    stddevSamp(false, "STDDEV_SAMP"),
    stddevSampStable(false),
    stochasticLinearRegression(false),
    stochasticLogisticRegression(false),
    studentTTest(false),
    sum(true, 1, true, 1, null),
    sumCount(false),
    sumKahan(false),
    sumMapFiltered(false),
    sumMapFilteredWithOverflow(false),
    sumMapWithOverflow(false),
    sumMappedArrays(false),
    sumWithOverflow(false),
    topK(false),
    topKWeighted(false),
    uniq(false),
    uniqCombined(false),
    uniqCombined64(false),
    uniqExact(false),
    uniqHLL12(false),
    uniqTheta(false),
    uniqUpTo(false),
    varPop(false, "VAR_POP"),
    varPopStable(false),
    varSamp(false, "VAR_SAMP"),
    varSampStable(false),
    welchTTest(false),
    windowFunnel(false);

    public static final Map<String, ClickHouseAggregateFunction> name2func;

    static {
        Map<String, ClickHouseAggregateFunction> map = new HashMap<>();
        String errorMsg = "[%s] is used by type [%s]";
        ClickHouseAggregateFunction used = null;
        for (ClickHouseAggregateFunction t : ClickHouseAggregateFunction.values()) {
            String name = t.name();
            if (!t.isCaseSensitive()) {
                name = name.toUpperCase();
            }
            used = map.put(name, t);
            if (used != null) {
                throw new IllegalStateException(String.format(Locale.ROOT, errorMsg, name, used.name()));
            }
        }

        name2func = Collections.unmodifiableMap(map);
    }

    /**
     * Converts given type name to corresponding aggregate function.
     *
     * @param function non-empty function
     * @return aggregate function
     */
    public static ClickHouseAggregateFunction of(String function) {
        if (function == null || (function = function.trim()).isEmpty()) {
            throw new IllegalArgumentException("Non-empty function is required");
        }

        ClickHouseAggregateFunction f = name2func.get(function);
        if (f == null) {
            f = name2func.get(function.toUpperCase()); // case-insensitive or just an alias
        }

        if (f == null) {
            throw new IllegalArgumentException("Unknown aggregate function: " + function);
        }
        return f;
    }

    private final boolean caseSensitive;
    private final int maxArgs;
    private final boolean singleValue;
    private final int refArgIndex;
    private final ClickHouseDataType valueType;
    private final List<String> aliases;

    ClickHouseAggregateFunction(boolean caseSensitive, String... aliases) {
        this(caseSensitive, 1, false, 0, ClickHouseDataType.Nothing, aliases);
    }

    ClickHouseAggregateFunction(boolean caseSensitive, int maxArgs, boolean singleValue, int refArgIndex,
            ClickHouseDataType valueType, String... aliases) {
        this.caseSensitive = caseSensitive;
        this.maxArgs = maxArgs < 0 ? 0 : maxArgs;
        this.singleValue = singleValue;
        this.refArgIndex = refArgIndex < 0 ? 0 : (refArgIndex > this.maxArgs ? this.maxArgs : refArgIndex);
        this.valueType = this.refArgIndex > 0 || valueType == null ? ClickHouseDataType.Nothing : valueType;
        if (aliases == null || aliases.length == 0) {
            this.aliases = Collections.emptyList();
        } else {
            this.aliases = Collections.unmodifiableList(Arrays.asList(aliases));
        }
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public List<String> getAliases() {
        return aliases;
    }
}
