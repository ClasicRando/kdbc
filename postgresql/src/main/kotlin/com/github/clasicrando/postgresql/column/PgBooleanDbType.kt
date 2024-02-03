package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.common.column.columnEncodeError
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeText
import kotlin.reflect.KClass

object PgBooleanDbType : DbType {

    override fun decode(type: ColumnData, value: String): Any = value == "t"
    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        return when (value) {
            is Boolean -> buffer.writeText(if (value) { "t" } else { "f" }, charset = charset)
            else -> columnEncodeError<Boolean>(value)
        }
    }

    override val encodeType: KClass<*> = Boolean::class
}
