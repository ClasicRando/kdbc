package com.github.clasicrando.sqlserver.column

import com.github.clasicrando.common.column.ColumnInfo
import com.github.clasicrando.common.column.DbType
import kotlin.reflect.KClass

object SqlServerBooleanDbType : DbType {
    override fun decode(type: ColumnInfo, value: String): Any {
        return value == "1"
    }

    override val encodeType: KClass<*> = Boolean::class
}
