package com.github.clasicrando.common.column

import com.github.clasicrando.common.datetime.DateTime
import kotlin.reflect.KClass

/**
 * [DbType] for database types that represent a date, time and timezone. Since the kotlinx-datetime
 * library does not contain a type for a datetime + timezone, [DateTime] is used to represent a
 * type with both attributes stored. Values are encoded and decoded as ISO-8601 datetime values.
 */
object DateTimeDbType : DbType {
    override fun decode(type: ColumnInfo, value: String): Any = DateTime.fromString(value)

    override val encodeType: KClass<*> = DateTime::class
}
