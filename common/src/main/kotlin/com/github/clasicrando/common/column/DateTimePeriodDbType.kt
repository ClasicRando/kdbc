package com.github.clasicrando.common.column

import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeText
import kotlinx.datetime.DateTimePeriod
import kotlin.reflect.KClass

/**
 * [DbType] for database types that describe a duration/interval of time. Values are encoded and
 * decoded as strings in the format of ISO-8601 durations.
 */
object DateTimePeriodDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = DateTimePeriod.parse(value)

    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        when (value) {
            is DateTimePeriod -> buffer.writeText(value.toString(), charset = charset)
            else -> columnEncodeError<DateTimePeriod>(value)
        }
    }

    override val encodeType: KClass<*> = DateTimePeriod::class
}
