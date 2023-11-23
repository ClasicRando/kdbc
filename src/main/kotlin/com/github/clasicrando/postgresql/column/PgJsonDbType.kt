package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.postgresql.type.PgJson
import kotlin.reflect.KClass

object PgJsonDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = PgJson(value)

    override val encodeType: KClass<*> = PgJson::class
}
