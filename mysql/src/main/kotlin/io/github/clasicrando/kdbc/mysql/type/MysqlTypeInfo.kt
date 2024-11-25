package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.result.ColumnFlags

public data class MysqlTypeInfo(
    val type: MySqlType,
    val flags: ColumnFlags,
) {
    internal companion object {
        fun fromColumnDefinition(columnDefinition: MysqlMessage.ColumnDefinition): MysqlTypeInfo {
            return MysqlTypeInfo(type = columnDefinition.type, flags = columnDefinition.flags)
        }
    }
}
