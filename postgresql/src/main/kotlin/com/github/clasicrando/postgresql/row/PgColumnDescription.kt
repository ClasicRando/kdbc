package com.github.clasicrando.postgresql.row

import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.postgresql.column.PgType

data class PgColumnDescription(
    override val fieldName: String,
    val tableOid: Int,
    val columnAttribute: Short,
    val pgType: PgType,
    val dataTypeSize: Short,
    val typeModifier: Int,
    val formatCode: Short,
) : ColumnData {
    override val dataType: Int = pgType.oidOrUnknown()
    override val typeName: String = fieldName
    override val typeSize: Long = dataTypeSize.toLong()
}
