package io.github.clasicrando.kdbc.core.query

import kotlin.reflect.typeOf

/** API to perform a single query against a database */
public class Query(public val sql: String) {
    private val parametersInner: MutableList<QueryParameter> = mutableListOf()

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

    override fun toString(): String = "Query.Prepared(sql='$sql', parameters=$parameters)"
}

public fun query(sql: String): Query = Query(sql)

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
