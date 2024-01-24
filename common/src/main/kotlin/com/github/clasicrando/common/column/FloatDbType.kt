package com.github.clasicrando.common.column

import kotlin.reflect.KClass

/**
 * [DbType] for database types that represent single precision floating point numbers. Values are
 * encoded and decoded as strings.
 */
object FloatDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = value.toFloat()

    override val encodeType: KClass<*> = Float::class
}
