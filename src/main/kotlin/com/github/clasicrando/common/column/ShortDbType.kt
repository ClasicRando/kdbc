package com.github.clasicrando.common.column

import kotlin.reflect.KClass

object ShortDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = value.toShort()

    override val encodeType: KClass<*> = Short::class
}
