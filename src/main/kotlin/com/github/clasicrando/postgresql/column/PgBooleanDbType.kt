package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnInfo
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.common.column.columnEncodeError
import kotlin.reflect.KClass

object PgBooleanDbType : DbType {
    override val supportsStringDecoding: Boolean get() = true

    override fun decode(type: ColumnInfo, value: String): Any = value == "t"

    override val encodeType: KClass<*> = Boolean::class

    override fun encode(value: Any): String {
        return when(value) {
            is Boolean -> if (value) { "t" } else { "f" }
            else -> columnEncodeError<Boolean>(value)
        }
    }
}
