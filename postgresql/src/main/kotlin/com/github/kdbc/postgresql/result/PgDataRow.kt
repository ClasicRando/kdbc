package com.github.kdbc.postgresql.result

import com.github.kdbc.core.column.ColumnDecodeError
import com.github.kdbc.core.column.checkOrColumnDecodeError
import com.github.kdbc.core.datetime.DateTime
import com.github.kdbc.core.result.DataRow
import com.github.kdbc.core.result.getAs
import com.github.kdbc.postgresql.column.PgType
import com.github.kdbc.postgresql.column.PgValue
import com.github.kdbc.postgresql.column.booleanTypeDecoder
import com.github.kdbc.postgresql.column.charTypeDecoder
import com.github.kdbc.postgresql.column.dateTimeTypeDecoder
import com.github.kdbc.postgresql.column.dateTypeDecoder
import com.github.kdbc.postgresql.column.doubleTypeDecoder
import com.github.kdbc.postgresql.column.floatTypeDecoder
import com.github.kdbc.postgresql.column.intTypeDecoder
import com.github.kdbc.postgresql.column.localDateTimeTypeDecoder
import com.github.kdbc.postgresql.column.longTypeDecoder
import com.github.kdbc.postgresql.column.shortTypeDecoder
import com.github.kdbc.postgresql.column.stringTypeDecoder
import com.github.kdbc.postgresql.column.timeTypeDecoder
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset

/**
 * Postgresql specific implementation for a [DataRow]. Uses the [rowBuffer] to extract data
 * returned from the postgresql server. Holds reference to the parent [resultSet] to look up
 * [PgResultSet.columnMap] when the user requires indexing the row by field name rather than
 * an [Int] index.
 */
internal class PgDataRow(
    private val rowBuffer: PgRowBuffer,
    private val resultSet: PgResultSet,
) : DataRow {
    /**
     * Collection of indexes that have already been processed. Since field buffers are read once,
     * the indexes that are already checked are cached to ensure buffers are only read once.
     */
    private val checkedIndex = mutableListOf<Int>()

    /**
     * Check to ensure the [index] is valid for this [resultSet] and has not been previously
     * checked.
     *
     * @throws IllegalArgumentException if the [index] has already been checked or the [index] can
     * not be found in the [resultSet]
     */
    private fun checkIndex(index: Int) {
        require(index !in checkedIndex) { "Index $index has already been read" }
        require(index in resultSet.columnMapping.indices) {
            val range = resultSet.columnMapping.indices
            "Index $index is not a valid index in this result. Values must be in $range"
        }
        checkedIndex.add(index)
    }

    /**
     * Get a [PgValue] for the specified [index], returning null if the value sent from the server
     * was a database NULL. The format code of the column specified by [index] decides if the value
     * returned is a [PgValue.Text] or [PgValue.Binary].
     *
     * @throws IllegalArgumentException if the [index] has already been checked or the [index] can
     * not be found in the [resultSet]
     */
    private fun getPgValue(index: Int): PgValue? {
        checkIndex(index)
        val buffer = rowBuffer.values[index] ?: return null
        val columnType = resultSet.columnMapping[index]
        return when (columnType.formatCode) {
            0.toShort() -> PgValue.Text(buffer, columnType)
            1.toShort() -> PgValue.Binary(buffer, columnType)
            else -> error(
                "Invalid format code from row description. Got ${columnType.formatCode}"
            )
        }
    }

    /**
     * Check the [pgValue] against the variable number of [compatibleTypes] to ensure the [PgType]
     * of the [pgValue] matches one of the [compatibleTypes].
     *
     * @throws IllegalArgumentException if [compatibleTypes] is empty
     * @throws ColumnDecodeError if the [PgType] of [pgValue] is not compatible with any of the
     * specified types
     */
    private inline fun <reified T> checkPgValue(pgValue: PgValue, vararg compatibleTypes: PgType) {
        require(compatibleTypes.isNotEmpty()) { "Cannot check against no PgType" }
        checkOrColumnDecodeError<T>(pgValue.typeData.pgType in compatibleTypes, pgValue.typeData)
    }

    override fun indexFromColumn(column: String): Int {
        val result = resultSet.columnMap[column]
        if (result != null) {
            return result
        }
        val columns = resultSet.columnMap.entries.joinToString { "${it.key}->${it.value}" }
        error("Could not find column in mapping. Column = '$column', columns = $columns")
    }

    override fun get(index: Int): Any? {
        val pgValue = getPgValue(index) ?: return null
        return resultSet.typeRegistry.decode(pgValue)
    }

    override fun getBoolean(index: Int): Boolean? {
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<Boolean>(pgValue, PgType.Bool, PgType.Int2, PgType.Int4)
        return booleanTypeDecoder.decode(pgValue)
    }

    override fun getByte(index: Int): Byte? {
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<Short>(pgValue, PgType.Char)
        return charTypeDecoder.decode(pgValue)
    }

    override fun getShort(index: Int): Short? {
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<Short>(pgValue, PgType.Int2)
        return shortTypeDecoder.decode(pgValue)
    }

    override fun getInt(index: Int): Int? {
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<Int>(pgValue, PgType.Int4, PgType.Int2, PgType.Oid)
        return when (pgValue.typeData.pgType) {
            is PgType.Int2 -> shortTypeDecoder.decode(pgValue).toInt()
            else -> intTypeDecoder.decode(pgValue)
        }
    }

    override fun getLong(index: Int): Long? {
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<Long>(pgValue, PgType.Int8, PgType.Int4, PgType.Int2, PgType.Oid)
        return when (pgValue.typeData.pgType) {
            is PgType.Int2 -> shortTypeDecoder.decode(pgValue).toLong()
            is PgType.Int4, is PgType.Oid -> intTypeDecoder.decode(pgValue).toLong()
            else -> longTypeDecoder.decode(pgValue)
        }
    }

    override fun getFloat(index: Int): Float? {
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<Float>(pgValue, PgType.Float4)
        return floatTypeDecoder.decode(pgValue)
    }

    override fun getDouble(index: Int): Double? {
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<Double>(pgValue, PgType.Float8, PgType.Float4)
        return when (pgValue.typeData.pgType) {
            is PgType.Int2 -> floatTypeDecoder.decode(pgValue).toDouble()
            else -> doubleTypeDecoder.decode(pgValue)
        }
    }

    override fun getLocalDate(index: Int): LocalDate? {
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<LocalDate>(pgValue, PgType.Date)
        return dateTypeDecoder.decode(pgValue)
    }

    override fun getLocalTime(index: Int): LocalTime? {
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<LocalTime>(pgValue, PgType.Time)
        return timeTypeDecoder.decode(pgValue)
    }

    override fun getLocalDateTime(index: Int): LocalDateTime? {
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<LocalDateTime>(pgValue, PgType.Timestamp)
        return localDateTimeTypeDecoder.decode(pgValue)
    }

    override fun getDateTime(index: Int): DateTime? {
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<DateTime>(pgValue, PgType.Timestamptz)
        return dateTimeTypeDecoder.decode(pgValue)
    }

    override fun getDateTime(index: Int, offset: UtcOffset): DateTime? {
        return getDateTime(index)?.withOffset(offset)
    }

    override fun getString(index: Int): String? {
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<String>(pgValue, PgType.Text, PgType.Varchar, PgType.Name, PgType.Bpchar)
        return stringTypeDecoder.decode(pgValue)
    }

    override fun <T> getList(index: Int): List<T?>? {
        return getAs<List<T>>(index)
    }

    override fun release() {
        rowBuffer.release()
    }
}
