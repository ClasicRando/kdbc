package com.github.clasicrando.common.column

import com.github.clasicrando.common.datetime.tryFromString
import kotlinx.datetime.LocalTime
import kotlin.reflect.KClass

/**
 * [DbType] for database types that represent time only values. Values are encoded and decoded as
 * strings. The expected format is ISO-8601.
 */
object LocalTimeDbType : DbType {
    override fun decode(type: ColumnInfo, value: String): Any {
        return LocalTime.tryFromString(value).getOrThrow()
    }

    override val encodeType: KClass<*> = LocalTime::class
}
