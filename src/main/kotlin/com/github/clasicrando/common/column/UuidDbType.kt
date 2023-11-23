package com.github.clasicrando.common.column

import java.util.UUID
import kotlin.reflect.KClass

object UuidDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = UUID.fromString(value)

    override val encodeType: KClass<*> = UUID::class
}
