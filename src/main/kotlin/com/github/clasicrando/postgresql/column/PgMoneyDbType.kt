package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnInfo
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.postgresql.type.PgMoney
import kotlin.reflect.KClass

object PgMoneyDbType : DbType {
    override fun decode(type: ColumnInfo, value: String): Any = PgMoney(value)

    override val encodeType: KClass<*> = PgMoney::class
}
