package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.common.column.columnEncodeError
import com.github.clasicrando.postgresql.type.PgJson
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeText
import kotlin.reflect.KClass

object PgJsonDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = PgJson(value)

    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        when (value) {
            is PgJson -> buffer.writeText(value.toString(), charset = charset)
            else -> columnEncodeError<PgJson>(value)
        }
    }

    override val encodeType: KClass<*> = PgJson::class
}
