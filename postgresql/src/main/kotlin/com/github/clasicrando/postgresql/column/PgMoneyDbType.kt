package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.common.column.columnEncodeError
import com.github.clasicrando.postgresql.type.PgMoney
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeText
import kotlin.reflect.KClass

object PgMoneyDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = PgMoney(value)

    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        when (value) {
            is PgMoney -> buffer.writeText(value.toString(), charset = charset)
            else -> columnEncodeError<PgMoney>(value)
        }
    }

    override val encodeType: KClass<*> = PgMoney::class
}
