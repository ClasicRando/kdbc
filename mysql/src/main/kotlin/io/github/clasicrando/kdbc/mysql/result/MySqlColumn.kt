package io.github.clasicrando.kdbc.mysql.result

import io.github.clasicrando.kdbc.mysql.type.MysqlTypeInfo

internal data class MySqlColumn(
    val ordinal: Long,
    val name: String,
    val typeInfo: MysqlTypeInfo,
)
