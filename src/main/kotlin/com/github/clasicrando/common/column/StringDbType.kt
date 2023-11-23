package com.github.clasicrando.common.column

import kotlin.reflect.KClass

object StringDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = value

    override val encodeType: KClass<*> = String::class

    override fun encode(value: Any): String = value as String
}
