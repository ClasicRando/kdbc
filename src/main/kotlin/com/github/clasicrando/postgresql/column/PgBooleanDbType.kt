package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.common.column.columnDecodeError
import com.github.clasicrando.common.column.columnEncodeError
import io.ktor.utils.io.charsets.Charset
import java.nio.ByteBuffer
import kotlin.reflect.KClass

object PgBooleanDbType : DbType {
    override val supportsStringDecoding: Boolean get() = true

    override fun decode(type: ColumnData, value: String): Any = value == "t"

    override val encodeType: KClass<*> = Boolean::class

    override fun encode(value: Any): String {
        return when(value) {
            is Boolean -> if (value) { "t" } else { "f" }
            else -> columnEncodeError<Boolean>(value)
        }
    }
}
