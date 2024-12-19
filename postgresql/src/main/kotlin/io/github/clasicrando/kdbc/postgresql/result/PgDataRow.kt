package io.github.clasicrando.kdbc.postgresql.result

import io.github.clasicrando.kdbc.core.column.checkOrColumnDecodeError
import io.github.clasicrando.kdbc.core.ensureNonNull
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.postgresql.column.PgColumnDescription
import io.github.clasicrando.kdbc.postgresql.column.PgFormatCode
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import io.github.clasicrando.kdbc.postgresql.exceptions.PgException
import io.github.clasicrando.kdbc.postgresql.type.BigDecimalTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.BigIntTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.BoolTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.ByteaTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.CharTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.IntTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.LocalDateTimeTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.LocalDateTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.LocalTimeTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.PgType
import io.github.clasicrando.kdbc.postgresql.type.PgTypeCache
import io.github.clasicrando.kdbc.postgresql.type.PgTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.SmallIntTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.VarcharTypeDescription
import io.ktor.utils.io.core.readBytes
import kotlinx.io.Buffer
import kotlinx.io.readString
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import kotlin.reflect.KType

/** Postgresql specific implementation for a [DataRow] */
internal class PgDataRow(
    private var pgValues: Array<PgValue?>,
    private val columnMapping: List<PgColumnDescription>,
    private val typeCache: PgTypeCache,
) : DataRow {
    /**
     * Check to ensure the [index] is valid for this row
     *
     * @throws IllegalArgumentException if the [index] can not be found in the [columnMapping]
     */
    private fun checkIndex(index: Int) {
        require(index in columnMapping.indices) {
            val range = columnMapping.indices
            "Index $index is not a valid index in this result. Values must be in $range"
        }
    }

    private fun getPgType(index: Int): PgType {
        checkIndex(index)
        return columnMapping[index].pgType
    }

    private fun <T : Any> decode(index: Int, deserializer: PgTypeDescription<T>): T? {
        val pgValue = pgValues[index] ?: return null
        return deserializer.decode(pgValue)
    }

    private fun <T : Any> tryDecode(index: Int, typeDescription: PgTypeDescription<T>): T? {
        val pgType = getPgType(index)
        if (typeDescription.dbType.oid == pgType.oid) {
            return decode(index, typeDescription)
        }
        val pgValue = pgValues[index] ?: return null
        checkOrColumnDecodeError(
            check = typeDescription.isCompatible(pgType),
            kType = typeDescription.kType,
            type = pgValue.typeData,
        ) {
            "Actual column type is not compatible with required type"
        }
        return decode(index, typeDescription)
    }

    override fun indexFromColumn(column: String): Int {
        val result = columnMapping.indexOfFirst { c -> c.fieldName == column }
        if (result >= 0) {
            return result
        }
        val columns = columnMapping.withIndex().joinToString { (i, c) -> "$i->${c.fieldName}" }
        error("Could not find column in mapping. Column = '$column', columns = $columns")
    }

    override fun get(index: Int, type: KType): Any? {
        val nonNullType = type.ensureNonNull()
        val typeDescription =
            typeCache.getTypeDescription<Any>(nonNullType)
                ?: throw PgException("Could not find type description for $nonNullType")
        return tryDecode(index, typeDescription)
    }

    override fun getBoolean(index: Int): Boolean? {
        return tryDecode(index, BoolTypeDescription)
    }

    override fun getByte(index: Int): Byte? {
        return tryDecode(index, CharTypeDescription)
    }

    override fun getShort(index: Int): Short? {
        return tryDecode(index, SmallIntTypeDescription)
    }

    override fun getInt(index: Int): Int? {
        return tryDecode(index, IntTypeDescription)
    }

    override fun getLong(index: Int): Long? {
        return tryDecode(index, BigIntTypeDescription)
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
        return tryDecode(index, typeCache.instantDescription)
    }

    override fun getOffsetDateTime(index: Int): OffsetDateTime? {
        return tryDecode(index, typeCache.offsetDateTimeDescription)
    }

    override fun getBigDecimal(index: Int): BigDecimal? {
        return tryDecode(index, BigDecimalTypeDescription)
    }

    override fun getBytes(index: Int): ByteArray? {
        return tryDecode(index, ByteaTypeDescription)
    }

    override fun getString(index: Int): String? {
        return tryDecode(index, VarcharTypeDescription)
    }

    internal companion object {
        fun fromBuffer(
            buffer: Buffer,
            columnMapping: List<PgColumnDescription>,
            typeCache: PgTypeCache,
        ): PgDataRow {
            val count = buffer.readShort()
            val pgValues =
                Array(count.toInt()) {
                    val length = buffer.readInt()
                    if (length < 0) {
                        return@Array null
                    }
                    val columnType = columnMapping[it]
                    when (columnType.formatCode) {
                        PgFormatCode.Text ->
                            PgValue.Text(
                                text = buffer.readString(byteCount = length.toLong()),
                                typeData = columnType,
                            )
                        PgFormatCode.Binary ->
                            PgValue.Binary(
                                bytes = buffer.readBytes(count = length),
                                typeData = columnType,
                            )
                    }
                }

            return PgDataRow(
                pgValues = pgValues,
                columnMapping = columnMapping,
                typeCache = typeCache,
            )
        }
    }
}
