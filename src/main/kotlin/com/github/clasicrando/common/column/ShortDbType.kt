package com.github.clasicrando.common.column

import kotlin.reflect.KClass

/**
 * [DbType] for database types that represent 2-byte (16-bit) integer numbers. Values are encoded
 * and decoded as strings.
 */
object ShortDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = value.toShort()

    override val encodeType: KClass<*> = Short::class
}
