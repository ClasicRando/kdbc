package com.github.clasicrando.common.column

import kotlinx.uuid.UUID
import kotlin.reflect.KClass

object UuidDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = UUID(value)

    override val encodeType: KClass<*> = UUID::class
}
