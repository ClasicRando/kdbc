package com.github.clasicrando.common.column

import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeText
import kotlin.reflect.KClass

/**
 * [DbType] for database types that represent 2-byte (16-bit) integer numbers. Values are encoded
 * and decoded as strings.
 */
object ShortDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = value.toShort()

    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        when (value) {
            is Short -> buffer.writeText(value.toString(), charset = charset)
            else -> columnEncodeError<Short>(value)
        }
    }

    override val encodeType: KClass<*> = Short::class
}
