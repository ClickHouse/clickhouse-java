package com.clickhouse.r2dbc;

import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.logging.Logger;
import com.clickhouse.logging.LoggerFactory;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.Parameter;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public class ClickHouseStatement implements Statement {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseStatement.class);

    private static final String NULL_VALUES_ARE_NOT_ALLOWED_AS_VALUE = "null values are not allowed as value.";
    private static final String CLASS_TYPES_ARE_NOT_ALLOWED_AS_VALUE = "class types are not allowed as value.";
    private static final String INVALID_PARAMETER_INDEX = "Invalid parameter index! Parameter index must be greater than 0.";

    private static final Object EXPLICITLY_SET_NULL_VALUE = new Object();
    public static final String NULL_VALUES_ARE_NOT_ALLOWED_AS_PARAMETER_NAME = "null values are not allowed as parameter name.";
    public static final String GENERATED_VALUES_CAN_NOT_BE_RETURNED_FROM_CLICKHOUSE_DATABASE = "Generated values can not be returned from Clickhouse database.";
    public static final String NON_EXISTING_IDENTIFIER_TEMPLATE = "non-existing identifier : %s";
    public static final String UNSUPPORTED_DATATYPE_BLOB = "Unsupported datatype: Blob";
    public static final String UNSUPPORTED_DATATYPE_CLOB = "Unsupported datatype: Clob";
    public static final String SQL_DOESN_T_HAVE_BINDING_PARAMETER_NAMES = "Sql doesn't have binding parameter names.";

    private final ClickHouseRequest<?> request;
    private final List<String> namedParameters;
    private final ClickHouseStatementBinding bindings;
    private int fetchSize;

    public ClickHouseStatement(String sql, ClickHouseRequest<?> request) {
        this.request = request.query(sql);
        namedParameters = request.getPreparedQuery().getParameters();
        bindings = new ClickHouseStatementBinding(namedParameters.size());
    }


    @Override
    public Statement add() {
        bindings.add();
        return this;
    }

    @Override
    public Statement bind(int identifierIndex, Object o) {
        if (o == null) {
            throw new IllegalArgumentException(NULL_VALUES_ARE_NOT_ALLOWED_AS_VALUE);
        } else if (o instanceof Class) {
            throw new IllegalArgumentException(CLASS_TYPES_ARE_NOT_ALLOWED_AS_VALUE);
        }

        if (identifierIndex < 0) {
            throw new IllegalArgumentException(INVALID_PARAMETER_INDEX);
        }

        bindings.addBinding(identifierIndex, safeValue(o));
        return this;
    }

    private Object safeValue(Object o) {
        if (o instanceof Blob) {
            throw new IllegalArgumentException(UNSUPPORTED_DATATYPE_BLOB);
        } else if (o instanceof Clob) {
            throw new IllegalArgumentException(UNSUPPORTED_DATATYPE_CLOB);
        } else if (o instanceof LocalDateTime) {
            LocalDateTime dateTime = (LocalDateTime) o;
            return (Timestamp.valueOf(dateTime).getTime() / 1000);
        } else if (o instanceof Parameter) {
            Object value = ((Parameter) o).getValue();
            if (value == null)
                return EXPLICITLY_SET_NULL_VALUE;
            return value;
        }
        return o;
    }

    @Override
    public Statement bind(String identifierName, Object o) {
        if (o == null) {
            throw new IllegalArgumentException(NULL_VALUES_ARE_NOT_ALLOWED_AS_VALUE);
        } else if (o instanceof Class) {
            throw new IllegalArgumentException(CLASS_TYPES_ARE_NOT_ALLOWED_AS_VALUE);
        } else if (namedParameters.isEmpty()) {
            throw new IllegalArgumentException(SQL_DOESN_T_HAVE_BINDING_PARAMETER_NAMES);
        }
        int index = namedParameters.indexOf(identifierName);
        if (index < 0) {
            throw new NoSuchElementException(String.format(NON_EXISTING_IDENTIFIER_TEMPLATE, identifierName));
        }
        bindings.addBinding(index, safeValue(o));
        return this;
    }

    @Override
    public Statement bindNull(int identifierIndex, Class<?> aClass) {
        if (identifierIndex < 0) {
            throw new IllegalArgumentException(INVALID_PARAMETER_INDEX);
        }
        bindings.addBinding(identifierIndex, EXPLICITLY_SET_NULL_VALUE);
        return this;
    }

    @Override
    public Statement bindNull(String identifierName, Class<?> aClass) {
        if (identifierName == null) {
            throw new IllegalArgumentException(NULL_VALUES_ARE_NOT_ALLOWED_AS_PARAMETER_NAME);
        }
        bindings.addBinding(namedParameters.indexOf(identifierName), EXPLICITLY_SET_NULL_VALUE);
        return this;
    }

    @Override
    public Statement fetchSize(int rows) {
        this.fetchSize = rows;
        return this;
    }

    @Override
    public Flux<? extends Result> execute() {
        List<ClickHouseStatementBinding.Binding> boundList = bindings.getBoundList();
        if (fetchSize > 0) {
            log.debug("setting fetch size {}", fetchSize);
            request.option(ClickHouseClientOption.MAX_RESULT_ROWS, fetchSize);
        }
        if (boundList.isEmpty()) {
            return Flux.from(Mono.fromFuture(request::execute)
                    .map(ClickHouseResult::new));
        } else {
            Stream<Mono<ClickHouseResponse>> monoStream = boundList.stream().map(binding -> {
                for (int i = 0; i < binding.values.length; i++ ) {
                    if (binding.values[i] == EXPLICITLY_SET_NULL_VALUE) {
                        binding.values[i] = null;
                    }
                }
                request.params(binding.values);
                return Mono.fromFuture(request::execute);
            });
            return Flux.fromStream(monoStream)
                    .flatMap(Mono::flux)
                    .map(ClickHouseResult::new);
        }
    }

    @Override
    public Statement returnGeneratedValues(String... columns) {
        return this;
    }
}
