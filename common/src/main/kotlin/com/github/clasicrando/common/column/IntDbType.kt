package com.github.clasicrando.common.column

import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeText
import kotlin.reflect.KClass

/**
 * [DbType] for database types that represent 4-byte (32-bit) integer numbers. Values are encoded
 * and decoded as strings.
 */
object IntDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = value.toInt()

    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        when (value) {
            is Int -> buffer.writeText(value.toString(), charset = charset)
            else -> columnEncodeError<Int>(value)
        }
    }

    override val encodeType: KClass<*> = Int::class
}
