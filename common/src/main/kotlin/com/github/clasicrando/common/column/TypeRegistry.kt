package com.github.clasicrando.common.column

import io.ktor.utils.io.charsets.Charset

interface TypeRegistry {
    fun decode(type: ColumnData, value: ByteArray, charset: Charset): Any

    fun encode(value: Any?): String?

    fun kindOf(value: Any?): Int
}
