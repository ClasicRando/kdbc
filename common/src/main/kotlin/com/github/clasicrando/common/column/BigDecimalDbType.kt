package com.github.clasicrando.common.column

import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.BytePacketBuilder
import io.ktor.utils.io.core.writeFully
import io.ktor.utils.io.core.writeText
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
    override fun decode(type: ColumnData, value: String): Any = value.toBigDecimal()

    override fun encode(value: Any, charset: Charset, buffer: BytePacketBuilder) {
        when (value) {
            is BigDecimal -> buffer.writeText(value.toString(), charset = charset)
            else -> columnEncodeError<BigDecimal>(value)
        }
    }

    override val encodeType: KClass<*> = BigDecimal::class
}
