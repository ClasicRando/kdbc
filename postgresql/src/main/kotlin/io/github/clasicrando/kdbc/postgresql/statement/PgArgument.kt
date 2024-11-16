package io.github.clasicrando.kdbc.postgresql.statement

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.postgresql.type.PgTypeCache
import io.github.clasicrando.kdbc.postgresql.type.PgTypeDescription

/** Argument for Postgres queries containing the [QueryParameter] and it's [PgTypeDescription] */
internal class PgArgument(
    val parameter: QueryParameter,
    val pgTypeDescription: PgTypeDescription<Any>,
) {
    constructor(
        parameter: QueryParameter,
        typeCache: PgTypeCache,
    ) : this(
        parameter = parameter,
        pgTypeDescription =
            typeCache.getTypeDescription(parameter.parameterType)
                ?: throw KdbcException(
                    "Could not find type description for ${parameter.parameterType}"
                ),
    )
}
