package io.github.clasicrando.kdbc.core.query

import kotlin.reflect.KType

data class QueryParameter(val value: Any?, val parameterType: KType)
