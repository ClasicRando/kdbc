package com.github.clasicrando.common.column

import io.ktor.utils.io.charsets.Charset
import java.nio.ByteBuffer
import kotlin.reflect.KClass

interface DbType {

    val supportsStringDecoding: Boolean get() = true

    fun decode(type: ColumnData, bytes: ByteArray, charset: Charset): Any {
        return decode(type, String(bytes, charset))
    }

    fun decode(type: ColumnData, value: String): Any

    val encodeType: KClass<*>

    fun encode(value: Any): String = value.toString()
}