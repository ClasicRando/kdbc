package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.postgresql.copy.PgBinaryCopyRow
import io.github.clasicrando.kdbc.postgresql.copy.PgCsvCopyRow
import io.github.clasicrando.kdbc.postgresql.statement.PgEncodeBuffer

data class CopyInTestRow(val id: Int, val textValue: String) : PgCsvCopyRow, PgBinaryCopyRow {
    override val values: List<Any?> = listOf(id, textValue)

    override val valueCount: Short = 2

    override fun encodeValues(buffer: PgEncodeBuffer) {
        buffer.encodeValue(id)
        buffer.encodeValue(textValue)
    }
}
