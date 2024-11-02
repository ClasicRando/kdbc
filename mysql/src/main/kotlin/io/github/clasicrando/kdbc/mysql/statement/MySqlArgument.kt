package io.github.clasicrando.kdbc.mysql.statement

import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.mysql.type.MySqlTypeDescription
import io.github.clasicrando.kdbc.mysql.type.MysqlTypeInfo

internal data class MySqlArgument(
    val value: QueryParameter,
    val typeInfo: MysqlTypeInfo,
    val typeDescription: MySqlTypeDescription<Any>,
)
