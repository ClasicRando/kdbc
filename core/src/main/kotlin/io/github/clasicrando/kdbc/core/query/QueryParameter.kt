package io.github.clasicrando.kdbc.core.query

import kotlin.reflect.KType

/**
 * Simple data class wrapping a query parameter's [value] and the type data about that parameter's
 * value as a [KType]
 */
data class QueryParameter(val value: Any?, val parameterType: KType)
