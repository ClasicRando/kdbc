package com.github.clasicrando.common.column

import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.ByteReadPacket

interface TypeRegistry<C : ColumnData> {
    fun decode(type: C, value: ByteArray, charset: Charset): Any

    fun encode(value: Any?): ByteReadPacket?

    fun kindOf(value: Any?): Int
}
