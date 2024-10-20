package io.github.clasicrando.kdbc.postgresql.copy

import io.github.clasicrando.kdbc.postgresql.statement.PgEncodeBuffer

/**
 * Implementors of this interface can supply its data fields as row fields for a binary copy
 * operation. Each row of the binary copy is a tuple of the number of columns (supplied as
 * [valueCount]) followed by each value binary encoded. The [PgEncodeBuffer] instance supplied to
 * [encodeValues] will handle transforming any value supplied into binary data.
 */
interface PgBinaryCopyRow {
    /** The number of values in the row. This must be a constant value for each type */
    val valueCount: Short

    /**
     * Encode all data fields of this class into the [buffer] supplied. Every implementation of
     * this method should simply call [PgEncodeBuffer.encodeValue] with each data field in the
     * order of the columns in the table.
     */
    fun encodeValues(buffer: PgEncodeBuffer)
}
