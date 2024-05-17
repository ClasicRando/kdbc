package io.github.clasicrando.kdbc.core.query

import kotlin.reflect.KType
import kotlin.reflect.typeOf

/**
 * Create a new [QueryParameter] for the provided [value]. Allows the reified type parameter to
 * provide the required [KType] for the parameter for convenience.
 */
inline fun <reified T : Any> QueryParameter(value: T?): QueryParameter {
    return QueryParameter(value, parameterType = typeOf<T>())
}

/**
 * Create a new [QueryParameter] for the provided [value]. Special case for a [List] of nullable
 * elements. Allows the reified type parameter to provide the required [KType] for the parameter
 * for convenience.
 */
@JvmName("QueryParameterNonNullItem")
inline fun <reified T : Any> QueryParameter(value: List<T?>): QueryParameter {
    return QueryParameter(value, parameterType = typeOf<List<T?>>())
}

/**
 * Create a new [QueryParameter] for the provided [value]. Special case for a [List] of non-null
 * elements. Allows the reified type parameter to provide the required [KType] for the parameter
 * for convenience.
 */
inline fun <reified T : Any> QueryParameter(value: List<T>): QueryParameter {
    return QueryParameter(value, parameterType = typeOf<List<T?>>())
}

/**
 * Simple data class wrapping a query parameter's [value] and the type data about that parameter's
 * value as a [KType]
 */
data class QueryParameter(val value: Any?, val parameterType: KType)
