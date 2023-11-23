package com.github.clasicrando.common.column

import kotlin.reflect.KClass

object DoubleDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = value.toDouble()

    override val encodeType: KClass<*> = Double::class
}
