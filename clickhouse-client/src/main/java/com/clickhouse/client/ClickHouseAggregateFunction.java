package com.clickhouse.client;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
    aggThrow(false), any(false), anyHeavy(false), anyLast(false), argMax(false), argMin(false), avg(true),
    avgWeighted(false), boundingRatio(false), categoricalInformationValue(false), corr(true), corrStable(false),
    count(true), covarPop(false, "COVAR_POP"), covarPopStable(false), covarSamp(false, "COVAR_SAMP"),
    covarSampStable(false), deltaSum(false), deltaSumTimestamp(false), dense_rank(false), entropy(false),
    first_value(true), groupArray(false), groupArrayInsertAt(false), groupArrayMovingAvg(false),
    groupArrayMovingSum(false), groupArraySample(false), groupBitAnd(false, "BIT_AND"), groupBitOr(false, "BIT_OR"),
    groupBitXor(false, "BIT_XOR"), groupBitmap(false), groupBitmapAnd(false), groupBitmapOr(false),
    groupBitmapXor(false), groupUniqArray(false), histogram(false), intervalLengthSum(false), kurtPop(false),
    kurtSamp(false), lagInFrame(false), last_value(true), leadInFrame(false), mannWhitneyUTest(false), max(true),
    maxIntersections(false), maxIntersectionsPosition(false), maxMap(false), min(true), minMap(false),
    quantile(false, "median"), quantileBFloat16(false, "medianBFloat16"),
    quantileDeterministic(false, "medianDeterministic"), quantileExact(false, "medianExact"),
    quantileExactExclusive(false), quantileExactHigh(false, "medianExactHigh"), quantileExactInclusive(false),
    quantileExactLow(false, "medianExactLow"), quantileExactWeighted(false, "medianExactWeighted"),
    quantileTDigest(false, "medianTDigest"), quantileTDigestWeighted(false, "medianTDigestWeighted"),
    quantileTiming(false, "medianTiming"), quantileTimingWeighted(false, "medianTimingWeighted"), quantiles(false),
    quantilesBFloat16(false), quantilesDeterministic(false), quantilesExact(false), quantilesExactExclusive(false),
    quantilesExactHigh(false), quantilesExactInclusive(false), quantilesExactLow(false), quantilesExactWeighted(false),
    quantilesTDigest(false), quantilesTDigestWeighted(false), quantilesTiming(false), quantilesTimingWeighted(false),
    rank(false), rankCorr(false), retention(false), row_number(false), sequenceCount(false), sequenceMatch(false),
    sequenceNextNode(false), simpleLinearRegression(false), skewPop(false), skewSamp(false),
    stddevPop(false, "STDDEV_POP"), stddevPopStable(false), stddevSamp(false, "STDDEV_SAMP"), stddevSampStable(false),
    stochasticLinearRegression(false), stochasticLogisticRegression(false), studentTTest(false), sum(true),
    sumCount(false), sumKahan(false), sumMap(false), sumMapFiltered(false), sumMapFilteredWithOverflow(false),
    sumMapWithOverflow(false), sumWithOverflow(false), topK(false), topKWeighted(false), uniq(false),
    uniqCombined(false), uniqCombined64(false), uniqExact(false), uniqHLL12(false), uniqTheta(false), uniqUpTo(false),
    varPop(false, "VAR_POP"), varPopStable(false), varSamp(false, "VAR_SAMP"), varSampStable(false), welchTTest(false),
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
    private final List<String> aliases;

    ClickHouseAggregateFunction(boolean caseSensitive, String... aliases) {
        this.caseSensitive = caseSensitive;
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
