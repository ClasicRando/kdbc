package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.postgresql.type.PgOid
import kotlin.reflect.KClass

object PgOidDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any = PgOid(value.toLong())

    override val encodeType: KClass<*> = PgOid::class
}
