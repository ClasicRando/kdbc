package com.github.clasicrando.postgresql.row

import com.github.clasicrando.common.result.AbstractMutableResultSet
import com.github.clasicrando.postgresql.column.PgTypeRegistry

internal class PgResultSet(
    val typeRegistry: PgTypeRegistry,
    columnMapping: List<PgColumnDescription>,
) : AbstractMutableResultSet<PgDataRow, PgColumnDescription>(columnMapping) {
    fun addRow(buffer: PgRowBuffer) {
        super.addRow(PgDataRow(buffer, this))
    }
}
