package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.mysql.result.ColumnFlags

internal data class MysqlTypeInfo(
    val type: MySqlType,
    val flags: ColumnFlags,
)
