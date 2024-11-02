package io.github.clasicrando.kdbc.postgresql.statement

import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.postgresql.type.PgTypeDescription

internal class PgArgument(
    val parameter: QueryParameter,
    val pgTypeDescription: PgTypeDescription<Any>,
)
