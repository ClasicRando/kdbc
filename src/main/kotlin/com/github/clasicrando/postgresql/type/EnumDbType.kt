package com.github.clasicrando.postgresql.type

import com.github.clasicrando.common.column.ColumnInfo
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.common.column.columnDecodeError
import kotlin.reflect.KClass

inline fun <reified E : Enum<E>> enumDbType(): DbType {
    return object : DbType {
        override val supportsStringDecoding: Boolean = true

        override fun decode(type: ColumnInfo, value: String): Any {
            return enumValues<E>().firstOrNull { it.name == value }
                ?: columnDecodeError(type, value)
        }

        override val encodeType: KClass<*> = E::class
    }
}
