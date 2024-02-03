package com.github.clasicrando.common.column

import com.github.clasicrando.common.datetime.tryFromString
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeText
import kotlinx.datetime.LocalTime
import kotlin.reflect.KClass

/**
 * [DbType] for database types that represent time only values. Values are encoded and decoded as
 * strings. The expected format is ISO-8601.
 */
object LocalTimeDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any {
        return LocalTime.tryFromString(value).getOrThrow()
    }

    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        when (value) {
            is LocalTime -> buffer.writeText(value.toString(), charset = charset)
            else -> columnEncodeError<LocalTime>(value)
        }
    }

    override val encodeType: KClass<*> = LocalTime::class
}
