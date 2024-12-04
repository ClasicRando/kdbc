package io.github.clasicrando.kdbc.core.query

import kotlin.reflect.KType
import kotlin.reflect.typeOf

/** Create a [QueryParameter] using the reified type [T] as the [KType] required */
public inline fun <reified T> QueryParameter(value: T?): QueryParameter {
    return QueryParameter(value, typeOf<T>())
}

/**
 * Simple data class wrapping a query parameter's [value] and the type data about that parameter's
 * value as a [KType]
 */
public data class QueryParameter(val value: Any?, val parameterType: KType)
