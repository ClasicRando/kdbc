package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.result.DataRow

/** Deserialization strategy for [DataRow] items into the required type [T] */
public fun interface RowParser<T : Any> {
    /**
     * Extract data from provided [row] to create a new instance of [T].
     *
     * @throws io.github.clasicrando.kdbc.core.exceptions.RowParseError if parser fails to convert
     *   the row into the desired type [T]
     */
    public fun fromRow(row: DataRow): T
}
