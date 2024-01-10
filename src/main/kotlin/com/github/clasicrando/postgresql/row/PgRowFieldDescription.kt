package com.github.clasicrando.postgresql.row

import com.github.clasicrando.common.column.ColumnInfo

data class PgRowFieldDescription(
    val fieldName: String,
    val tableOid: Int,
    val columnAttribute: Short,
    val dataTypeOid: Int,
    val dataTypeSize: Short,
    val typeModifier: Int,
    val formatCode: Short,
) : ColumnInfo {
    override val dataType: Int = dataTypeOid
    override val name: String = fieldName
    override val typeSize: Long = dataTypeSize.toLong()
}
