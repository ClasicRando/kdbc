package io.github.clasicrando.kdbc.mysql.result

import io.github.clasicrando.kdbc.core.column.ColumnMetadata
import io.github.clasicrando.kdbc.mysql.type.MysqlTypeInfo

public data class MySqlColumn(
    val ordinal: Long,
    val name: String,
    val typeInfo: MysqlTypeInfo,
) : ColumnMetadata {
    override val dataType: Int = typeInfo.type.inner

    override val fieldName: String = name

    override val typeName: String = typeInfo.type.name
}
