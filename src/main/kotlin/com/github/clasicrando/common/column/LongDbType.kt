package com.github.clasicrando.common.column

import kotlin.reflect.KClass

/**
 * [DbType] for database types that represent 8-byte (64-bit) integer numbers. Values are encoded
 * and decoded as strings.
 */
object LongDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = value.toLong()

    override val encodeType: KClass<*> = Long::class
}
