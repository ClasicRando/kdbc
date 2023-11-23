package com.github.clasicrando.common.column

import com.github.clasicrando.common.datetime.DateTime
import kotlin.reflect.KClass

object DateTimeDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = DateTime.fromString(value)

    override val encodeType: KClass<*> = DateTime::class
}
