package com.github.clasicrando.common.column

import kotlin.reflect.KClass

/**
 * [DbType] for database types that are raw 8-bit values. Values are encoded and decoded as
 * strings.
 */
object ByteDbType : DbType {
    override val encodeType: KClass<*> = Byte::class

    override fun decode(type: ColumnInfo, value: String): Any = value.toByte()
}