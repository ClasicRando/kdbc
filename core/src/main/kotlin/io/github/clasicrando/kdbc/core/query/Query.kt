package io.github.clasicrando.kdbc.core.query

import kotlin.reflect.typeOf

/** API to perform a single query against a database */
public class Query(public val sql: String) {
    private var parametersInner: MutableList<QueryParameter> = mutableListOf()

    public val parameters: List<QueryParameter>
        get() = parametersInner

    /**
     * Bind a next [parameter] to the [Query]. This adds the parameter to the internal list of
     * parameters in the order the parameter exists in the query regardless of the vendor specific
     * method of linking parameter values to query parameters.
     *
     * Returns a reference to the same object to allow for method chaining.
     */
    public fun bind(parameter: QueryParameter): Query {
        parametersInner.add(parameter)
        return this
    }

    public fun bindMany(parameters: List<QueryParameter>): Query {
        if (parametersInner.isEmpty()) {
            parametersInner = parameters as? MutableList ?: ArrayList(parameters)
            return this
        }
        parametersInner.addAll(parameters)
        return this
    }

    /** Clears all parameters previously bound */
    public fun clearParameters() {
        parametersInner.clear()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Query) return false

        if (sql != other.sql) return false
        if (parameters != other.parameters) return false

        return true
    }

    override fun hashCode(): Int {
        var result = sql.hashCode()
        result = 31 * result + parameters.hashCode()
        return result
    }

    override fun toString(): String {
        return "Query(sql='$sql', parameters=$parameters)"
    }
}

public fun query(sql: String): Query = Query(sql)

private val booleanType = typeOf<Boolean>()

public fun Query.bind(parameter: Boolean?): Query {
    return bind(QueryParameter(parameter, booleanType))
}

private val byteType = typeOf<Byte>()

public fun Query.bind(parameter: Byte?): Query {
    return bind(QueryParameter(parameter, byteType))
}

private val shortType = typeOf<Short>()

public fun Query.bind(parameter: Short?): Query {
    return bind(QueryParameter(parameter, shortType))
}

private val intType = typeOf<Int>()

public fun Query.bind(parameter: Int?): Query {
    return bind(QueryParameter(parameter, intType))
}

private val longType = typeOf<Long>()

public fun Query.bind(parameter: Long?): Query {
    return bind(QueryParameter(parameter, longType))
}

private val floatType = typeOf<Float>()

public fun Query.bind(parameter: Float?): Query {
    return bind(QueryParameter(parameter, floatType))
}

private val doubleType = typeOf<Double>()

public fun Query.bind(parameter: Double?): Query {
    return bind(QueryParameter(parameter, doubleType))
}

private val localTimeType = typeOf<java.time.LocalTime>()

public fun Query.bind(parameter: java.time.LocalTime?): Query {
    return bind(QueryParameter(parameter, localTimeType))
}

private val localDateType = typeOf<java.time.LocalDate>()

public fun Query.bind(parameter: java.time.LocalDate?): Query {
    return bind(QueryParameter(parameter, localDateType))
}

private val localDateTimeType = typeOf<java.time.LocalDateTime>()

public fun Query.bind(parameter: java.time.LocalDateTime?): Query {
    return bind(QueryParameter(parameter, localDateTimeType))
}

private val instantType = typeOf<java.time.Instant>()

public fun Query.bind(parameter: java.time.Instant?): Query {
    return bind(QueryParameter(parameter, instantType))
}

private val offsetDateTimeType = typeOf<java.time.OffsetDateTime>()

public fun Query.bind(parameter: java.time.OffsetDateTime?): Query {
    return bind(QueryParameter(parameter, offsetDateTimeType))
}

private val bigDecimalType = typeOf<java.math.BigDecimal>()

public fun Query.bind(parameter: java.math.BigDecimal?): Query {
    return bind(QueryParameter(parameter, bigDecimalType))
}

private val byteArrayType = typeOf<ByteArray>()

public fun Query.bind(parameter: ByteArray?): Query {
    return bind(QueryParameter(parameter, byteArrayType))
}

private val stringType = typeOf<String>()

public fun Query.bind(parameter: String?): Query {
    return bind(QueryParameter(parameter, stringType))
}

/**
 * Extension method to call [Query.bind] and construct the [QueryParameter] using the utility
 * methods that construct the required type data implicitly.
 */
public inline fun <reified T : Any> Query.bind(parameter: T?): Query {
    return bind(QueryParameter(value = parameter, parameterType = typeOf<T>()))
}

/**
 * Extension method to call [Query.bind] and construct the [QueryParameter] using the utility
 * methods that construct the required type data implicitly. Special case for a [List] of nullable
 * elements.
 */
@JvmName("QueryParameterNonNullItem")
public inline fun <reified T : Any> Query.bind(parameter: List<T?>): Query {
    return bind(QueryParameter(value = parameter, parameterType = typeOf<List<T?>>()))
}

/**
 * Extension method to call [Query.bind] and construct the [QueryParameter] using the utility
 * methods that construct the required type data implicitly. Special case for a [List] of non-null
 * elements.
 */
public inline fun <reified T : Any> Query.bind(parameter: List<T>): Query {
    return bind(QueryParameter(value = parameter, parameterType = typeOf<List<T>>()))
}

/**
 * Bind an out parameter used to call a stored procedure with the type [T] used for query planning
 * and a null placeholder input. The value of the out parameter will be provided as a row in the
 * query result so this statement is just to satisfy prepared query parsing and planning.
 */
public inline fun <reified T : Any> Query.bindOut(): Query {
    return bind(QueryParameter(value = null, typeOf<T>()))
}
