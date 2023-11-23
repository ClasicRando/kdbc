package com.github.clasicrando.common.column

import kotlinx.datetime.DateTimePeriod
import kotlin.reflect.KClass

object DateTimePeriodDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = DateTimePeriod.parse(value)

    override val encodeType: KClass<*> = DateTimePeriod::class
}
