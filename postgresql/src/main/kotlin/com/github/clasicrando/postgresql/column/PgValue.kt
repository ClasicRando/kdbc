package com.github.clasicrando.postgresql.column

import com.github.clasicrando.postgresql.row.PgColumnDescription
import kotlinx.io.Source
import kotlinx.io.asSource
import kotlinx.io.buffered
import kotlinx.io.readString
import java.io.ByteArrayInputStream

sealed class PgValue(val bytes: Source, val typeData: PgColumnDescription) {
    class Text private constructor(
        private val innerText: String?,
        private val innerBytes: Source?,
        typeData: PgColumnDescription,
    ) : PgValue(
        bytes = innerBytes
            ?: innerText!!.toByteArray()
                .let { ByteArrayInputStream(it).asSource() }
                .buffered(),
        typeData = typeData,
    ) {
        constructor(bytes: Source, typeData: PgColumnDescription) : this(null, bytes, typeData)
        constructor(text: String, typeData: PgColumnDescription) : this(text, null, typeData)
        val text get() = innerText ?: innerBytes!!.readString()

        override fun toString(): String {
            return "PgValue.Text(text=$text, typeData=$typeData)"
        }
    }
    class Binary(bytes: Source, typeData: PgColumnDescription): PgValue(bytes, typeData) {
        override fun toString(): String {
            return "PgValue.Binary(typeData=$typeData)"
        }
    }
}
