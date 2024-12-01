package io.github.clasicrando.kdbc.mysql.statement

import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.mysql.type.MySqlTypeCache
import io.github.clasicrando.kdbc.mysql.type.MySqlTypeDescription

/**
 * MySQL prepared statement parameter holding the [QueryParameter] and the associated
 * [MySqlTypeDescription] for encoding.
 */
internal data class MySqlArgument(
    val value: QueryParameter,
    val typeDescription: MySqlTypeDescription<Any>,
) {
    constructor(
        value: QueryParameter,
        typeCache: MySqlTypeCache,
    ) : this(value, typeCache.getTypeDescription(value.parameterType))
}
