package com.github.clasicrando.common.column

import kotlin.reflect.KClass

/**
 * [DbType] for database types that represent 4-byte (32-bit) integer numbers. Values are encoded
 * and decoded as strings.
 */
object IntDbType : DbType {
    override val encodeType: KClass<*> = Int::class

    override fun decode(type: ColumnData, value: String): Any = value.toInt()
}
