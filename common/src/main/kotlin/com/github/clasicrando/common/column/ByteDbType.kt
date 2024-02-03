package com.github.clasicrando.common.column

import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import kotlin.reflect.KClass

/**
 * [DbType] for database types that are raw 8-bit values. Values are encoded and decoded as
 * strings.
 */
object ByteDbType : DbType {
    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        when (value) {
            is Byte -> buffer.writeByte(value)
            else -> columnEncodeError<Byte>(value)
        }
    }

    override val encodeType: KClass<*> = Byte::class

    override fun decode(type: ColumnData, value: String): Any = value.toByte()
}