package com.github.clasicrando.common.column

import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeText
import kotlin.reflect.KClass

/**
 * [DbType] for database types that represent 8-byte (64-bit) integer numbers. Values are encoded
 * and decoded as strings.
 */
object LongDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = value.toLong()

    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        when (value) {
            is Long -> buffer.writeText(value.toString(), charset = charset)
            else -> columnEncodeError<Long>(value)
        }
    }

    override val encodeType: KClass<*> = Long::class
}
