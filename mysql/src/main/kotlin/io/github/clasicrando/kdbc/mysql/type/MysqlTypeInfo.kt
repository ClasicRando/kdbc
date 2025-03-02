package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.core.connection.IntBitFlags
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

/** Minimally required type info from a [MysqlMessage.ColumnDefinition] */
public data class MysqlTypeInfo(
    /** General MySQL type info */
    val type: MySqlType,
    /** Column flags providing more details about the column's expected type */
    val flags: IntBitFlags,
) {
    internal companion object {
        fun fromColumnDefinition(columnDefinition: MysqlMessage.ColumnDefinition): MysqlTypeInfo {
            return MysqlTypeInfo(type = columnDefinition.type, flags = columnDefinition.flags)
        }
    }
}
