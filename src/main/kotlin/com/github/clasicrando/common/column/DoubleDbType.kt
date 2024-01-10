package com.github.clasicrando.common.column

import kotlin.reflect.KClass

/**
 * [DbType] for database types that represent double precision floating point numbers. Values are
 * encoded and decoded as strings.
 */
object DoubleDbType : DbType {
    override fun decode(type: ColumnInfo, value: String): Any = value.toDouble()

    override val encodeType: KClass<*> = Double::class
}
