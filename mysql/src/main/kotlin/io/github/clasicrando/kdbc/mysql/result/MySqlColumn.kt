package io.github.clasicrando.kdbc.mysql.result

import io.github.clasicrando.kdbc.core.column.ColumnMetadata
import io.github.clasicrando.kdbc.mysql.type.MysqlTypeInfo

/** Column data within a result set */
public data class MySqlColumn(
    /** Column index (0-based) */
    val ordinal: Long,
    /** Column name for lookup */
    val name: String,
    /** Type info of the column */
    val typeInfo: MysqlTypeInfo,
) : ColumnMetadata {
    override val dataType: Int = typeInfo.type.inner

    override val fieldName: String = name

    override val typeName: String = typeInfo.type.name
}
