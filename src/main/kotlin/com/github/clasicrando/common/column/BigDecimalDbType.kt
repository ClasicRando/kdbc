package com.github.clasicrando.common.column

import java.math.BigDecimal
import kotlin.reflect.KClass

object BigDecimalDbType : DbType {
    override val encodeType: KClass<*> = BigDecimal::class

    override fun decode(type: ColumnData, value: String): Any = value.toBigDecimal()
}
