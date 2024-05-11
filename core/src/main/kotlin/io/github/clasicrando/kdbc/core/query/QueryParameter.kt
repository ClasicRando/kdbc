package io.github.clasicrando.kdbc.core.query

import kotlin.reflect.KType
import kotlin.reflect.typeOf

inline fun <reified T : Any> QueryParameter(value: T?): QueryParameter {
    return QueryParameter(value, parameterType = typeOf<T>())
}

@JvmName("QueryParameterNonNullItem")
inline fun <reified T : Any> QueryParameter(value: List<T?>): QueryParameter {
    return QueryParameter(value, parameterType = typeOf<List<T?>>())
}

inline fun <reified T : Any> QueryParameter(value: List<T>): QueryParameter {
    return QueryParameter(value, parameterType = typeOf<List<T?>>())
}

data class QueryParameter(val value: Any?, val parameterType: KType)
