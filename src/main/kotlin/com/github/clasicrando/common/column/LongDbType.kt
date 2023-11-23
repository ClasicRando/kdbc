package com.github.clasicrando.common.column

import kotlin.reflect.KClass

object LongDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = value.toLong()

    override val encodeType: KClass<*> = Long::class
}
