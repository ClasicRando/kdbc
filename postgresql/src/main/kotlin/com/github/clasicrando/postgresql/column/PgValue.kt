package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.common.buffer.ReadBufferSlice
import com.github.clasicrando.common.buffer.readText

sealed class PgValue(val typeData: PgColumnDescription) {
    class Text @PublishedApi internal constructor(
        val text: String,
        typeData: PgColumnDescription,
    ) : PgValue(typeData) {
        constructor(bytes: ReadBuffer, typeData: PgColumnDescription)
                : this(bytes.readText(), typeData)

        override fun toString(): String {
            return "PgValue.Text(text=$text, typeData=$typeData)"
        }
    }
    class Binary @PublishedApi internal constructor(
        val bytes: ReadBufferSlice,
        typeData: PgColumnDescription,
    ) : PgValue(typeData) {
        override fun toString(): String {
            return "PgValue.Binary(typeData=$typeData)"
        }
    }
}
