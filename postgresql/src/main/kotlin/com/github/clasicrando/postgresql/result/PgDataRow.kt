package com.github.clasicrando.postgresql.result

import com.github.clasicrando.common.column.checkOrColumnDecodeError
import com.github.clasicrando.common.column.columnDecodeError
import com.github.clasicrando.common.datetime.DateTime
import com.github.clasicrando.common.result.DataRow
import com.github.clasicrando.common.result.getAs
import com.github.clasicrando.postgresql.column.PgType
import com.github.clasicrando.postgresql.column.PgValue
import com.github.clasicrando.postgresql.column.booleanTypeDecoder
import com.github.clasicrando.postgresql.column.dateTimeTypeDecoder
import com.github.clasicrando.postgresql.column.dateTypeDecoder
import com.github.clasicrando.postgresql.column.doubleTypeDecoder
import com.github.clasicrando.postgresql.column.floatTypeDecoder
import com.github.clasicrando.postgresql.column.intTypeDecoder
import com.github.clasicrando.postgresql.column.localDateTimeTypeDecoder
import com.github.clasicrando.postgresql.column.longTypeDecoder
import com.github.clasicrando.postgresql.column.shortTypeDecoder
import com.github.clasicrando.postgresql.column.stringTypeDecoder
import com.github.clasicrando.postgresql.column.timeTypeDecoder
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime

internal class PgDataRow(
    private val rowBuffer: PgRowBuffer,
    private val resultSet: PgResultSet,
) : DataRow {
    private val checkedIndex = mutableListOf<Int>()

    private fun checkIndex(index: Int) {
        require(index !in checkedIndex) { "Index $index has already been read" }
        require(index in resultSet.columnMapping.indices) {
            val range = resultSet.columnMapping.indices
            "Index $index is not a valid index in this result. Values must be in $range"
        }
        checkedIndex.add(index)
    }

    private fun getPgValue(index: Int): PgValue? {
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
        checkIndex(index)
        val pgValue = getPgValue(index) ?: return null
        return resultSet.typeRegistry.decode(pgValue)
    }

    override fun getBoolean(index: Int): Boolean? {
        checkIndex(index)
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<Boolean>(pgValue, PgType.Bool, PgType.Int2, PgType.Int4)
        return booleanTypeDecoder.decode(pgValue)
    }

    override fun getByte(index: Int): Byte? {
        checkIndex(index)
        val pgValue = getPgValue(index) ?: return null
        columnDecodeError<Byte>(
            type = pgValue.typeData,
            reason = "Cannot fetch a Byte value since Postgresql does not have a tinyint type",
        )
    }

    override fun getShort(index: Int): Short? {
        checkIndex(index)
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<Short>(pgValue, PgType.Int2)
        return shortTypeDecoder.decode(pgValue)
    }

    override fun getInt(index: Int): Int? {
        checkIndex(index)
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<Int>(pgValue, PgType.Int4, PgType.Int2, PgType.Oid)
        return when (pgValue.typeData.pgType) {
            is PgType.Int2 -> shortTypeDecoder.decode(pgValue).toInt()
            else -> intTypeDecoder.decode(pgValue)
        }
    }

    override fun getLong(index: Int): Long? {
        checkIndex(index)
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<Long>(pgValue, PgType.Int8, PgType.Int4, PgType.Int2, PgType.Oid)
        return when (pgValue.typeData.pgType) {
            is PgType.Int2 -> shortTypeDecoder.decode(pgValue).toLong()
            is PgType.Int4, is PgType.Oid -> intTypeDecoder.decode(pgValue).toLong()
            else -> longTypeDecoder.decode(pgValue)
        }
    }

    override fun getFloat(index: Int): Float? {
        checkIndex(index)
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<Float>(pgValue, PgType.Float4)
        return floatTypeDecoder.decode(pgValue)
    }

    override fun getDouble(index: Int): Double? {
        checkIndex(index)
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<Double>(pgValue, PgType.Float8, PgType.Float4)
        return when (pgValue.typeData.pgType) {
            is PgType.Int2 -> floatTypeDecoder.decode(pgValue).toDouble()
            else -> doubleTypeDecoder.decode(pgValue)
        }
    }

    override fun getLocalDate(index: Int): LocalDate? {
        checkIndex(index)
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<LocalDate>(pgValue, PgType.Date)
        return dateTypeDecoder.decode(pgValue)
    }

    override fun getLocalTime(index: Int): LocalTime? {
        checkIndex(index)
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<LocalTime>(pgValue, PgType.Time)
        return timeTypeDecoder.decode(pgValue)
    }

    override fun getLocalDateTime(index: Int): LocalDateTime? {
        checkIndex(index)
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<LocalDateTime>(pgValue, PgType.Timestamp)
        return localDateTimeTypeDecoder.decode(pgValue)
    }

    override fun getDateTime(index: Int): DateTime? {
        checkIndex(index)
        val pgValue = getPgValue(index) ?: return null
        checkPgValue<DateTime>(pgValue, PgType.Timestamptz)
        return dateTimeTypeDecoder.decode(pgValue)
    }

    override fun getString(index: Int): String? {
        checkIndex(index)
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
