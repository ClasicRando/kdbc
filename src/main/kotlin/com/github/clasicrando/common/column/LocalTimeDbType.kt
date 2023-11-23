package com.github.clasicrando.common.column

import com.github.clasicrando.common.datetime.tryFromString
import kotlinx.datetime.LocalTime
import kotlin.reflect.KClass

object LocalTimeDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any {
        return LocalTime.tryFromString(value).getOrThrow()
    }

    override val encodeType: KClass<*> = LocalTime::class
}
