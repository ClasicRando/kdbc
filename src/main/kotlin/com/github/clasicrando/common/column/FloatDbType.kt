package com.github.clasicrando.common.column

import kotlin.reflect.KClass

object FloatDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = value.toFloat()

    override val encodeType: KClass<*> = Float::class
}
