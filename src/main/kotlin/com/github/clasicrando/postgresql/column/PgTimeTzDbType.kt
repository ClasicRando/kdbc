package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.postgresql.type.PgTimeTz
import kotlin.reflect.KClass

object PgTimeTzDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = PgTimeTz.fromString(value)

    override val encodeType: KClass<*> = PgTimeTz::class
}
