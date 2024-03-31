package com.github.clasicrando.postgresql.result

import com.github.clasicrando.common.result.AbstractMutableResultSet
import com.github.clasicrando.common.result.ResultSet
import com.github.clasicrando.postgresql.column.PgColumnDescription
import com.github.clasicrando.postgresql.column.PgTypeRegistry

/**
 * Postgresql implementation of a [ResultSet] where the rows are represents as [PgRowBuffer]s.
 * This also keeps reference to a [typeRegistry] instance for the purpose of decoding non-standard
 * types using the lookup maps for type decoders.
 */
internal class PgResultSet(
    val typeRegistry: PgTypeRegistry,
    columnMapping: List<PgColumnDescription>,
) : AbstractMutableResultSet<PgDataRow, PgColumnDescription>(columnMapping) {
    fun addRow(buffer: PgRowBuffer) {
        super.addRow(PgDataRow(buffer, this))
    }
}
