package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.common.column.columnDecodeError
import com.github.clasicrando.common.column.columnEncodeError
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeText
import kotlin.reflect.KClass

object PgCharDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any {
        if (value.length != 1) {
            columnDecodeError(type, value)
        }
        return value.first()
    }

    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        when (value) {
            is Char -> buffer.writeText(value.toString(), charset = charset)
            else -> columnEncodeError<Char>(value)
        }
    }

    override val encodeType: KClass<*> = Char::class
}
