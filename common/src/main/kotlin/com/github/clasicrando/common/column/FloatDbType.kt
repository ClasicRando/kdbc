package com.github.clasicrando.common.column

import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeText
import kotlin.reflect.KClass

/**
 * [DbType] for database types that represent single precision floating point numbers. Values are
 * encoded and decoded as strings.
 */
object FloatDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = value.toFloat()

    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        when (value) {
            is Float -> buffer.writeText(value.toString(), charset = charset)
            else -> columnEncodeError<Float>(value)
        }
    }

    override val encodeType: KClass<*> = Float::class
}
