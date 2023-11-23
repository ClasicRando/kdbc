package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.common.column.columnDecodeError
import kotlin.reflect.KClass

inline fun <reified E : Enum<E>> enumDbType(): DbType {
    return EnumDbType(E::class) { type, value ->
        enumValues<E>().firstOrNull { it.name == value } ?: columnDecodeError(type, value)
    }
}

class EnumDbType<E : Enum<E>> @PublishedApi internal constructor(
    enumClass: KClass<E>,
    val decoder: (ColumnData, String) -> E,
) : DbType {
    override val supportsStringDecoding: Boolean = true

    override fun decode(type: ColumnData, value: String): Any {
        return decoder(type, value)
    }

    override val encodeType: KClass<*> = enumClass
}
