package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.postgresql.type.PgInet
import kotlin.reflect.KClass

object PgInetDbType : DbType {
    override fun decode(type: ColumnData, value: String): Any {
        return PgInet(value)
    }

    override val encodeType: KClass<*> = PgInet::class

    override fun encode(value: Any): String = (value as PgInet).address
}
