package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnInfo
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.common.column.columnDecodeError
import kotlin.reflect.KClass

object PgCharDbType : DbType {
    override fun decode(type: ColumnInfo, value: String): Any {
        if (value.length != 1) {
            columnDecodeError(type, value)
        }
        return value.first()
    }

    override val encodeType: KClass<*> = Char::class
}
