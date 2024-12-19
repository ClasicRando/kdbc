package io.github.clasicrando.kdbc.mysql.result

import io.github.clasicrando.kdbc.core.column.checkOrColumnDecodeError
import io.github.clasicrando.kdbc.core.ensureNonNull
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.mysql.exceptions.MySqlException
import io.github.clasicrando.kdbc.mysql.type.BigDecimalTypeDescription
import io.github.clasicrando.kdbc.mysql.type.BooleanTypeDescription
import io.github.clasicrando.kdbc.mysql.type.ByteArrayTypeDescription
import io.github.clasicrando.kdbc.mysql.type.IntegerTypeDescription
import io.github.clasicrando.kdbc.mysql.type.LocalDateTimeTypeDescription
import io.github.clasicrando.kdbc.mysql.type.LocalDateTypeDescription
import io.github.clasicrando.kdbc.mysql.type.LocalTimeTypeDescription
import io.github.clasicrando.kdbc.mysql.type.LongTypeDescription
import io.github.clasicrando.kdbc.mysql.type.MySqlTypeCache
import io.github.clasicrando.kdbc.mysql.type.MySqlTypeDescription
import io.github.clasicrando.kdbc.mysql.type.MysqlTypeInfo
import io.github.clasicrando.kdbc.mysql.type.ShortTypeDescription
import io.github.clasicrando.kdbc.mysql.type.StringTypeDescription
import io.github.clasicrando.kdbc.mysql.type.TinyIntTypeDescription
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import kotlin.reflect.KType

/**
 * [DataRow] implementation for MySQL. Stored each [MySqlValue] in the row as well as the associated
 * [MySqlColumn] info and the [MySqlTypeCache] to lookup type descriptions for decoding.
 */
internal class MySqlDataRow(
    private val values: Array<MySqlValue?>,
    private val columns: List<MySqlColumn>,
    private val typeCache: MySqlTypeCache,
) : DataRow {
    /** Quick lookup map for column names to the corresponding index */
    private val columnNames =
        columns.asSequence().mapIndexed { index, column -> column.name to index }.toMap()

    override fun indexFromColumn(column: String): Int {
        val index = columnNames[column]
        if (index != null) {
            return index
        }

        val columnCollection = columns.withIndex().joinToString { (i, c) -> "$i->${c.name}" }
        return columnNames[column]
            ?: throw MySqlException(
                "Could not find column in mapping. Column = '$column', columns = $columnCollection"
            )
    }

    private fun <T : Any> tryDecode(index: Int, typeDescription: MySqlTypeDescription<T>): T? {
        val mySqlType = getMySqlType(index)
        if (typeDescription.dbType.inner == mySqlType.type.inner) {
            return decode(index, typeDescription)
        }
        checkOrColumnDecodeError(
            check = typeDescription.isCompatible(mySqlType.type),
            kType = typeDescription.kType,
            type = columns[index],
        ) {
            "Actual column type is not compatible with required type"
        }
        return decode(index, typeDescription)
    }

    override fun get(index: Int, type: KType): Any? {
        val nonNullType = type.ensureNonNull()
        val typeDescription = typeCache.getTypeDescription<Any>(nonNullType)
        return tryDecode(index, typeDescription)
    }

    override fun getBoolean(index: Int): Boolean? {
        return tryDecode(index, BooleanTypeDescription)
    }

    override fun getByte(index: Int): Byte? {
        return tryDecode(index, TinyIntTypeDescription)
    }

    override fun getShort(index: Int): Short? {
        return tryDecode(index, ShortTypeDescription)
    }

    override fun getInt(index: Int): Int? {
        return tryDecode(index, IntegerTypeDescription)
    }

    override fun getLong(index: Int): Long? {
        return tryDecode(index, LongTypeDescription)
    }

    override fun getLocalTime(index: Int): LocalTime? {
        return tryDecode(index, LocalTimeTypeDescription)
    }

    override fun getLocalDate(index: Int): LocalDate? {
        return tryDecode(index, LocalDateTypeDescription)
    }

    override fun getLocalDateTime(index: Int): LocalDateTime? {
        return tryDecode(index, LocalDateTimeTypeDescription)
    }

    override fun getInstant(index: Int): Instant? {
        return tryDecode(index, typeCache.instantTypeDescription)
    }

    override fun getOffsetDateTime(index: Int): OffsetDateTime? {
        return tryDecode(index, typeCache.offsetDateTimeTypeDescription)
    }

    override fun getBigDecimal(index: Int): BigDecimal? {
        return tryDecode(index, BigDecimalTypeDescription)
    }

    override fun getBytes(index: Int): ByteArray? {
        return tryDecode(index, ByteArrayTypeDescription)
    }

    override fun getString(index: Int): String? {
        return tryDecode(index, StringTypeDescription)
    }

    /**
     * Check to ensure the [index] is valid for this row
     *
     * @throws IllegalArgumentException if the [index] can not be found in the [columns]
     */
    private fun checkIndex(index: Int) {
        require(index in columns.indices) {
            val range = columns.indices
            "Index $index is not a valid index in this result. Values must be in $range"
        }
    }

    private fun getMySqlType(index: Int): MysqlTypeInfo {
        checkIndex(index)
        return columns[index].typeInfo
    }

    private fun <T : Any> decode(index: Int, deserializer: MySqlTypeDescription<T>): T? {
        val pgValue = values[index] ?: return null
        return deserializer.decode(pgValue)
    }

    override fun toString(): String {
        return "MySqlDataRow([${values.joinToString()}])"
    }
}
