package com.github.clasicrando.common.column

import kotlin.reflect.KClass

object ByteDbType : DbType {
    override val encodeType: KClass<*> = Byte::class

    override fun decode(type: ColumnData, value: String): Any = value.toByte()
}