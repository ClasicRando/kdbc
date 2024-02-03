package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.common.column.columnEncodeError
import com.github.clasicrando.postgresql.type.PgInet
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeText
import kotlin.reflect.KClass

object PgInetDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any {
        return PgInet(value)
    }

    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        when (value) {
            is PgInet -> buffer.writeText(value.address, charset = charset)
            else -> columnEncodeError<PgInet>(value)
        }
    }

    override val encodeType: KClass<*> = PgInet::class
}
