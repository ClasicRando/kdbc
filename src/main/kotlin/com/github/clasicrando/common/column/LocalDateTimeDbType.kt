package com.github.clasicrando.common.column

import com.github.clasicrando.common.datetime.tryFromString
import kotlinx.datetime.LocalDateTime
import kotlin.reflect.KClass

object LocalDateTimeDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any {
        return LocalDateTime.tryFromString(value).getOrThrow()
    }

    override val encodeType: KClass<*> = LocalDateTime::class
}
