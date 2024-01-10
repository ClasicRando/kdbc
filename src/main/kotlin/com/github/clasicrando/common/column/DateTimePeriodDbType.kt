package com.github.clasicrando.common.column

import kotlinx.datetime.DateTimePeriod
import kotlin.reflect.KClass

/**
 * [DbType] for database types that describe a duration/interval of time. Values are encoded and
 * decoded as strings in the format of ISO-8601 durations.
 */
object DateTimePeriodDbType : DbType {
    override fun decode(type: ColumnInfo, value: String): Any = DateTimePeriod.parse(value)

    override val encodeType: KClass<*> = DateTimePeriod::class
}
