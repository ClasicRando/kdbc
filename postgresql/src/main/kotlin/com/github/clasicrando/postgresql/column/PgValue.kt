package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.buffer.ArrayReadBuffer
import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.common.buffer.readText
import com.github.clasicrando.postgresql.row.PgColumnDescription

sealed class PgValue(val bytes: ReadBuffer, val typeData: PgColumnDescription) {
    class Text private constructor(
        private val innerText: String?,
        private val innerBytes: ReadBuffer?,
        typeData: PgColumnDescription,
    ) : PgValue(
        bytes = innerBytes
            ?: innerText!!.toByteArray()
                .let { object : ArrayReadBuffer(it) {} },
        typeData = typeData,
    ) {
        constructor(bytes: ReadBuffer, typeData: PgColumnDescription) : this(null, bytes, typeData)
        constructor(text: String, typeData: PgColumnDescription) : this(text, null, typeData)
        val text get() = innerText ?: innerBytes!!.readText()

        override fun toString(): String {
            return "PgValue.Text(text=$text, typeData=$typeData)"
        }
    }
    class Binary(bytes: ReadBuffer, typeData: PgColumnDescription): PgValue(bytes, typeData) {
        override fun toString(): String {
            return "PgValue.Binary(typeData=$typeData)"
        }
    }
}
