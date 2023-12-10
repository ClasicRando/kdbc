package com.github.clasicrando.common.column

import com.github.clasicrando.common.datetime.tryFromString
import kotlinx.datetime.LocalDate
import kotlin.reflect.KClass

/**
 * [DbType] for database types that represent date only values. Values are encoded and decoded as
 * strings. The expected format is ISO-8601.
 */
object LocalDateDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any {
        return LocalDate.tryFromString(value).getOrThrow()
    }

    override val encodeType: KClass<*> = LocalDate::class
}
