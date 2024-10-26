package io.github.clasicrando.kdbc.postgresql.result

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.result.AbstractMutableResultSet
import io.github.clasicrando.kdbc.postgresql.column.PgColumnDescription
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import io.github.clasicrando.kdbc.postgresql.type.PgTypeCache

/**
 * Postgresql implementation of a [io.github.clasicrando.kdbc.core.result.ResultSet] where the rows
 * are represents as [PgDataRow]s. This also keeps reference to a [typeCache] instance for the
 * purpose of decoding non-standard types using the lookup maps for type decoders.
 */
internal class PgResultSet(
    private val typeCache: PgTypeCache,
    columnMapping: List<PgColumnDescription>,
) : AbstractMutableResultSet<PgDataRow, PgColumnDescription>(columnMapping) {
    fun addRow(buffer: ByteReadBuffer) {
        val count = buffer.readShort()
        val pgValues =
            Array(count.toInt()) {
                val length = buffer.readInt()
                if (length < 0) {
                    return@Array null
                }
                val slice = buffer.slice(length)
                val columnType = columnMapping[it]
                when (val formatCode = columnType.formatCode) {
                    0.toShort() -> PgValue.Text(slice, columnType)
                    1.toShort() -> PgValue.Binary(slice, columnType)
                    else -> error("Invalid format code from row description. Got $formatCode")
                }
            }

        val row =
            PgDataRow(
                rowBuffer = buffer,
                pgValues = pgValues,
                columnMapping = columnMapping,
                typeCache = typeCache,
            )
        super.addRow(row)
    }
}
