package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.buffer.ByteReadBuffer

sealed class PgValue(val typeData: PgColumnDescription) {
    class Text @PublishedApi internal constructor(
        val text: String,
        typeData: PgColumnDescription,
    ) : PgValue(typeData) {
        constructor(bytes: ByteReadBuffer, typeData: PgColumnDescription)
                : this(bytes.readText(), typeData)

        override fun toString(): String {
            return "PgValue.Text(text=$text, typeData=$typeData)"
        }
    }
    class Binary @PublishedApi internal constructor(
        val bytes: ByteReadBuffer,
        typeData: PgColumnDescription,
    ) : PgValue(typeData) {
        override fun toString(): String {
            return "PgValue.Binary(typeData=$typeData)"
        }
    }
}
