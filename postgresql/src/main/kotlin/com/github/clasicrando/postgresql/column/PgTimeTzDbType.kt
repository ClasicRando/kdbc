package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.common.column.columnEncodeError
import com.github.clasicrando.postgresql.type.PgTimeTz
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeText
import kotlin.reflect.KClass

object PgTimeTzDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = PgTimeTz.fromString(value)

    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        when (value) {
            is PgTimeTz -> buffer.writeText(value.toString(), charset = charset)
            else -> columnEncodeError<PgTimeTz>(value)
        }
    }

    override val encodeType: KClass<*> = PgTimeTz::class
}
