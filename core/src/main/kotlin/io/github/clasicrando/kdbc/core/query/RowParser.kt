package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.exceptions.RowParseError
import io.github.clasicrando.kdbc.core.result.DataRow

/** Implementors deserialize strategy for [DataRow] items into the required type [T] */
interface RowParser<T : Any> {
    /**
     * Extract data from provided [row] to create a new instance of [T].
     *
     * @throws RowParseError if parser fails to convert the row into the desired type [T]
     */
    fun fromRow(row: DataRow): T
}
