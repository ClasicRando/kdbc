package io.github.clasicrando.kdbc.postgresql.copy

import io.github.clasicrando.kdbc.postgresql.statement.PgEncodeBuffer

interface PgBinaryCopyRow {
    val valueCount: Short
    fun encodeValues(buffer: PgEncodeBuffer)
}
