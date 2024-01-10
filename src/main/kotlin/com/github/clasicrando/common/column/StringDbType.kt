package com.github.clasicrando.common.column

import kotlin.reflect.KClass

/**
 * [DbType] for database types that represent variable length character arrays. This includes text,
 * varchar, char, nvarchar, nchar and other assorted database types. Values are decoded as string
 * and encoded by simply passing forward the [String] reference.
 */
object StringDbType : DbType {
    override fun decode(type: ColumnInfo, value: String): Any = value

    override val encodeType: KClass<*> = String::class

    override fun encode(value: Any): String = value as String
}
