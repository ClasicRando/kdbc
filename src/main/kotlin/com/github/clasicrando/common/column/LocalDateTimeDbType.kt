package com.github.clasicrando.common.column

import com.github.clasicrando.common.datetime.tryFromString
import kotlinx.datetime.LocalDateTime
import kotlin.reflect.KClass

/**
 * [DbType] for database types that represent date time values without a timezone. Values are
 * encoded and decoded as strings. The expected format is ISO-8601.
 */
object LocalDateTimeDbType : DbType {
    override fun decode(type: ColumnInfo, value: String): Any {
        return LocalDateTime.tryFromString(value).getOrThrow()
    }

    override val encodeType: KClass<*> = LocalDateTime::class
}
