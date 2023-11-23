package com.github.clasicrando.common.column

import kotlin.reflect.KClass

object IntDbType : DbType {
    override val encodeType: KClass<*> = Int::class

    override fun decode(type: ColumnData, value: String): Any = value.toInt()
}
