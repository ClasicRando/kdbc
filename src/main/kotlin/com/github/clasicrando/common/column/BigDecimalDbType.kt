package com.github.clasicrando.common.column

import java.math.BigDecimal
import kotlin.reflect.KClass

/**
 * [DbType] for database types that require high precision numbers (i.e. float or double do not
 * provide the precision required). Values are encoded and decoded as strings.
 *
 * TODO
 * - decouple from the java [BigDecimal] and use a kotlinx library
 */
object BigDecimalDbType : DbType {
    override val encodeType: KClass<*> = BigDecimal::class

    override fun decode(type: ColumnInfo, value: String): Any = value.toBigDecimal()
}
