package com.github.clasicrando.common.column

import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeText
import kotlin.reflect.KClass

/**
 * [DbType] for database types that represent double precision floating point numbers. Values are
 * encoded and decoded as strings.
 */
object DoubleDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = value.toDouble()

    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        when (value) {
            is Double -> buffer.writeText(value.toString(), charset = charset)
            else -> columnEncodeError<Double>(value)
        }
    }

    override val encodeType: KClass<*> = Double::class
}
