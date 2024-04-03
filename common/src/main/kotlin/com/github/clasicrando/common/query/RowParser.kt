package com.github.clasicrando.common.query

import com.github.clasicrando.common.datetime.DateTime
import com.github.clasicrando.common.exceptions.RowParseError
import com.github.clasicrando.common.result.DataRow
import com.github.clasicrando.common.result.getBoolean
import com.github.clasicrando.common.result.getByte
import com.github.clasicrando.common.result.getDateTime
import com.github.clasicrando.common.result.getDouble
import com.github.clasicrando.common.result.getFloat
import com.github.clasicrando.common.result.getInt
import com.github.clasicrando.common.result.getList
import com.github.clasicrando.common.result.getLocalDate
import com.github.clasicrando.common.result.getLocalDateTime
import com.github.clasicrando.common.result.getLocalTime
import com.github.clasicrando.common.result.getLong
import com.github.clasicrando.common.result.getShort
import com.github.clasicrando.common.result.getString
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset

/** Implementors deserialize strategy for [DataRow] items into the required type [T] */
interface RowParser<T : Any> {
    /**
     * Extract data from provided [row] to create a new instance of [T].
     *
     * @throws RowParseError if parser fails to convert the row into the desired type [T]
     */
    fun fromRow(row: DataRow): T
}

/**
 * Extract the [dataRow] [Boolean] value at the specified [index], throwing a [RowParseError] if
 * the value is null. Calls [DataRow.getBoolean] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getBooleanOrThrow(dataRow: DataRow, index: Int): Boolean {
    return dataRow.getBoolean(index)
        ?: throw RowParseError(this, reason = "Column index $index must be non-null")
}

/**
 * Extract the [dataRow] [Byte] value at the specified [index], throwing a [RowParseError] if the
 * value is null. Calls [DataRow.getByte] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getByteOrThrow(dataRow: DataRow, index: Int): Byte {
    return dataRow.getByte(index)
        ?: throw RowParseError(this, reason = "Column index $index must be non-null")
}

/**
 * Extract the [dataRow] [Short] value at the specified [index], throwing a [RowParseError] if the
 * value is null. Calls [DataRow.getShort] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getShortOrThrow(dataRow: DataRow, index: Int): Short {
    return dataRow.getShort(index)
        ?: throw RowParseError(this, reason = "Column index $index must be non-null")
}

/**
 * Extract the [dataRow] [Int] value at the specified [index], throwing a [RowParseError] if the
 * value is null. Calls [DataRow.getInt] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getIntOrThrow(dataRow: DataRow, index: Int): Int {
    return dataRow.getInt(index)
        ?: throw RowParseError(this, reason = "Column index $index must be non-null")
}

/**
 * Extract the [dataRow] [Long] value at the specified [index], throwing a [RowParseError] if the
 * value is null. Calls [DataRow.getLong] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getLongOrThrow(dataRow: DataRow, index: Int): Long {
    return dataRow.getLong(index)
        ?: throw RowParseError(this, reason = "Column index $index must be non-null")
}

/**
 * Extract the [dataRow] [Float] value at the specified [index], throwing a [RowParseError] if the
 * value is null. Calls [DataRow.getFloat] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getFloatOrThrow(dataRow: DataRow, index: Int): Float {
    return dataRow.getFloat(index)
        ?: throw RowParseError(this, reason = "Column index $index must be non-null")
}

/**
 * Extract the [dataRow] [Double] value at the specified [index], throwing a [RowParseError] if the
 * value is null. Calls [DataRow.getDouble] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getDoubleOrThrow(dataRow: DataRow, index: Int): Double {
    return dataRow.getDouble(index)
        ?: throw RowParseError(this, reason = "Column index $index must be non-null")
}

/**
 * Extract the [dataRow] [LocalDate] value at the specified [index], throwing a [RowParseError] if
 * the value is null. Calls [DataRow.getLocalDate] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getLocalDateOrThrow(dataRow: DataRow, index: Int): LocalDate {
    return dataRow.getLocalDate(index)
        ?: throw RowParseError(this, reason = "Column index $index must be non-null")
}

/**
 * Extract the [dataRow] [LocalTime] value at the specified [index], throwing a [RowParseError] if
 * the value is null. Calls [DataRow.getLocalTime] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getLocalTimeOrThrow(dataRow: DataRow, index: Int): LocalTime {
    return dataRow.getLocalTime(index)
        ?: throw RowParseError(this, reason = "Column index $index must be non-null")
}

/**
 * Extract the [dataRow] [LocalDateTime] value at the specified [index], throwing a [RowParseError]
 * if the value is null. Calls [DataRow.getLocalDateTime] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getLocalDateTimeOrThrow(dataRow: DataRow, index: Int): LocalDateTime {
    return dataRow.getLocalDateTime(index)
        ?: throw RowParseError(this, reason = "Column index $index must be non-null")
}

/**
 * Extract the [dataRow] [DateTime] value at the specified [index], throwing a [RowParseError] if
 * the value is null. Calls [DataRow.getDateTime] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getDateTimeOrThrow(dataRow: DataRow, index: Int): DateTime {
    return dataRow.getDateTime(index)
        ?: throw RowParseError(this, reason = "Column index $index must be non-null")
}

/**
 * Extract the [dataRow] [DateTime] value at the specified [index], throwing a [RowParseError] if
 * the value is null. Calls [DataRow.getDateTime] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getDateTimeOrThrow(
    dataRow: DataRow,
    index: Int,
    offset: UtcOffset,
): DateTime {
    return dataRow.getDateTime(index, offset)
        ?: throw RowParseError(this, reason = "Column index $index must be non-null")
}

/**
 * Extract the [dataRow] [String] value at the specified [index], throwing a [RowParseError] if
 * the value is null. Calls [DataRow.getString] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getStringOrThrow(dataRow: DataRow, index: Int): String {
    return dataRow.getString(index)
        ?: throw RowParseError(this, reason = "Column index $index must be non-null")
}

/**
 * Extract the [dataRow] [List] value at the specified [index], throwing a [RowParseError] if
 * the value is null. Calls [DataRow.getList] to extract the nullable value.
 */
fun <T : Any, E : Any> RowParser<T>.getList(dataRow: DataRow, index: Int): List<E?> {
    return dataRow.getList(index)
        ?: throw RowParseError(this, reason = "Column index $index must be non-null")
}







/**
 * Extract the [dataRow] [Boolean] value at the specified [name], throwing a [RowParseError] if
 * the value is null. Calls [DataRow.getBoolean] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getBooleanOrThrow(dataRow: DataRow, name: String): Boolean {
    return dataRow.getBoolean(name)
        ?: throw RowParseError(this, reason = "Column name '$name' must be non-null")
}

/**
 * Extract the [dataRow] [Byte] value at the specified [name], throwing a [RowParseError] if the
 * value is null. Calls [DataRow.getByte] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getByteOrThrow(dataRow: DataRow, name: String): Byte {
    return dataRow.getByte(name)
        ?: throw RowParseError(this, reason = "Column name '$name' must be non-null")
}

/**
 * Extract the [dataRow] [Short] value at the specified [name], throwing a [RowParseError] if the
 * value is null. Calls [DataRow.getShort] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getShortOrThrow(dataRow: DataRow, name: String): Short {
    return dataRow.getShort(name)
        ?: throw RowParseError(this, reason = "Column name '$name' must be non-null")
}

/**
 * Extract the [dataRow] [Int] value at the specified [name], throwing a [RowParseError] if the
 * value is null. Calls [DataRow.getInt] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getIntOrThrow(dataRow: DataRow, name: String): Int {
    return dataRow.getInt(name)
        ?: throw RowParseError(this, reason = "Column name '$name' must be non-null")
}

/**
 * Extract the [dataRow] [Long] value at the specified [name], throwing a [RowParseError] if the
 * value is null. Calls [DataRow.getLong] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getLongOrThrow(dataRow: DataRow, name: String): Long {
    return dataRow.getLong(name)
        ?: throw RowParseError(this, reason = "Column name '$name' must be non-null")
}

/**
 * Extract the [dataRow] [Float] value at the specified [name], throwing a [RowParseError] if the
 * value is null. Calls [DataRow.getFloat] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getFloatOrThrow(dataRow: DataRow, name: String): Float {
    return dataRow.getFloat(name)
        ?: throw RowParseError(this, reason = "Column name '$name' must be non-null")
}

/**
 * Extract the [dataRow] [Double] value at the specified [name], throwing a [RowParseError] if the
 * value is null. Calls [DataRow.getDouble] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getDoubleOrThrow(dataRow: DataRow, name: String): Double {
    return dataRow.getDouble(name)
        ?: throw RowParseError(this, reason = "Column name '$name' must be non-null")
}

/**
 * Extract the [dataRow] [LocalDate] value at the specified [name], throwing a [RowParseError] if
 * the value is null. Calls [DataRow.getLocalDate] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getLocalDateOrThrow(dataRow: DataRow, name: String): LocalDate {
    return dataRow.getLocalDate(name)
        ?: throw RowParseError(this, reason = "Column name '$name' must be non-null")
}

/**
 * Extract the [dataRow] [LocalTime] value at the specified [name], throwing a [RowParseError] if
 * the value is null. Calls [DataRow.getLocalTime] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getLocalTimeOrThrow(dataRow: DataRow, name: String): LocalTime {
    return dataRow.getLocalTime(name)
        ?: throw RowParseError(this, reason = "Column name '$name' must be non-null")
}

/**
 * Extract the [dataRow] [LocalDateTime] value at the specified [name], throwing a [RowParseError]
 * if the value is null. Calls [DataRow.getLocalDateTime] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getLocalDateTimeOrThrow(dataRow: DataRow, name: String): LocalDateTime {
    return dataRow.getLocalDateTime(name)
        ?: throw RowParseError(this, reason = "Column name '$name' must be non-null")
}

/**
 * Extract the [dataRow] [DateTime] value at the specified [name], throwing a [RowParseError] if
 * the value is null. Calls [DataRow.getDateTime] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getDateTimeOrThrow(dataRow: DataRow, name: String): DateTime {
    return dataRow.getDateTime(name)
        ?: throw RowParseError(this, reason = "Column name '$name' must be non-null")
}

/**
 * Extract the [dataRow] [DateTime] value at the specified [name], throwing a [RowParseError] if
 * the value is null. Calls [DataRow.getDateTime] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getDateTimeOrThrow(
    dataRow: DataRow,
    name: String,
    offset: UtcOffset,
): DateTime {
    return dataRow.getDateTime(name, offset)
        ?: throw RowParseError(this, reason = "Column name '$name' must be non-null")
}

/**
 * Extract the [dataRow] [String] value at the specified [name], throwing a [RowParseError] if
 * the value is null. Calls [DataRow.getString] to extract the nullable value.
 */
fun <T : Any> RowParser<T>.getStringOrThrow(dataRow: DataRow, name: String): String {
    return dataRow.getString(name)
        ?: throw RowParseError(this, reason = "Column name '$name' must be non-null")
}

/**
 * Extract the [dataRow] [List] value at the specified [name], throwing a [RowParseError] if
 * the value is null. Calls [DataRow.getList] to extract the nullable value.
 */
fun <T : Any, E : Any> RowParser<T>.getList(dataRow: DataRow, name: String): List<E?> {
    return dataRow.getList(name)
        ?: throw RowParseError(this, reason = "Column name '$name' must be non-null")
}
