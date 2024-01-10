package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnInfo
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.postgresql.type.PgJson
import kotlin.reflect.KClass

object PgJsonDbType : DbType {
    override fun decode(type: ColumnInfo, value: String): Any = PgJson(value)

    override val encodeType: KClass<*> = PgJson::class
}
