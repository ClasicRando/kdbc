package com.github.clasicrando.common.result

import com.github.clasicrando.common.AutoRelease
import com.github.clasicrando.common.column.ColumnDecodeError
import com.github.clasicrando.common.datetime.DateTime
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime

/**
 * Representation of a [ResultSet] row. Allows for fetching of each field's value by index or
 * column name. Once a value has been fetched from the row (by index or name) the value cannot be
 * fetched again. Doing as such will throw an [IllegalArgumentException].
 *
 * This type is not thread safe and should be accessed by a single thread or coroutine to ensure
 * consistent processing of data.
 */
interface DataRow : AutoRelease {
    /**
     * Return the index of the specified [column] name.
     *
     * @throws IllegalArgumentException [column] name cannot be found in the row
     */
    fun indexFromColumn(column: String): Int

    /**
     * Get the value stored within the field at the [index] specified.
     *
     * @throws IllegalArgumentException if the index is out of range of the row or the field has
     * already been decoded
     */
    operator fun get(index: Int): Any?

    /**
     * Get the value stored within the field of [column] specified.
     *
     * @throws IllegalArgumentException if the index is out of range of the row or the field has
     * already been decoded
     */
    operator fun get(column: String): Any? = get(indexFromColumn(column))

    /**
     * Get the value stored within the field at the [index] specified, attempting to coerce the
     * underlining value to a [Boolean].
     *
     * @throws IllegalArgumentException if the index is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getBoolean(index: Int): Boolean?

    /**
     * Get the value stored within the field at the [index] specified as a [Byte].
     *
     * @throws IllegalArgumentException if the index is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getByte(index: Int): Byte?

    /**
     * Get the value stored within the field at the [index] specified as a [Short]. If the
     * underling value in the field is a [Byte], an implicit conversion is performed.
     *
     * @throws IllegalArgumentException if the index is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getShort(index: Int): Short?

    /**
     * Get the value stored within the field at the [index] specified as an [Int]. If the underling
     * value in the field is a [Byte] or [Short], an implicit conversion is performed.
     *
     * @throws IllegalArgumentException if the index is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getInt(index: Int): Int?

    /**
     * Get the value stored within the field at the [index] specified as a [Long]. If the underling
     * value in the field is a [Byte], [Short] or [Int], an implicit conversion is performed.
     *
     * @throws IllegalArgumentException if the index is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getLong(index: Int): Long?

    /**
     * Get the value stored within the field at the [index] specified as a [Float].
     *
     * @throws IllegalArgumentException if the index is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getFloat(index: Int): Float?

    /**
     * Get the value stored within the field at the [index] specified as a [Double]. If the
     * underlining value in the field is a [Float], an implicit conversion is performed.
     *
     * @throws IllegalArgumentException if the index is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getDouble(index: Int): Double?

    /**
     * Get the value stored within the field at the [index] specified, performing a non-safe cast to
     * [LocalDate].
     *
     * @throws IllegalArgumentException if the index is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getLocalDate(index: Int): LocalDate?

    /**
     * Get the value stored within the field at the [index] specified, performing a non-safe cast to
     * [LocalTime].
     *
     * @throws IllegalArgumentException if the index is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getLocalTime(index: Int): LocalTime?

    /**
     * Get the value stored within the field at the [index] specified, performing a non-safe cast to
     * [LocalDateTime].
     *
     * @throws IllegalArgumentException if the index is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getLocalDateTime(index: Int): LocalDateTime?

    /**
     * Get the value stored within the field at the [index] specified, performing a non-safe cast to
     * [DateTime].
     *
     * @throws IllegalArgumentException if the index is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getDateTime(index: Int): DateTime?

    /**
     * Get the value stored within the field at the [index] specified, performing a non-safe cast to
     * [String].
     *
     * @throws IllegalArgumentException if the index is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getString(index: Int): String?

    /**
     *
     *
     * @throws IllegalArgumentException if the index is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun <T> getList(index: Int): List<T?>?
}

/**
 * Get the value stored within the field at the [index] specified, performing a non-safe cast to
 * the desired type [T]. Throws an [IllegalArgumentException] if the index is out of range of the
 * row.
 */
@Suppress("UNCHECKED_CAST")
fun <T : Any> DataRow.getAs(index: Int): T? = get(index) as T?

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to the
 * desired type [T]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun <T : Any> DataRow.getAs(column: String): T? = getAs(indexFromColumn(column))

/**
 * Get the value stored within the field of [column] specified, attempting to coerce the
 * underlining value to a [Boolean]. Throws an [IllegalArgumentException] if the index is out of
 * range of the row.
 */
fun DataRow.getBoolean(column: String): Boolean? = getBoolean(indexFromColumn(column))

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [Byte]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getByte(column: String): Byte? = getByte(indexFromColumn(column))

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [Short]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getShort(column: String): Short? = getShort(indexFromColumn(column))

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [Int]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getInt(column: String): Int? = getInt(indexFromColumn(column))

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [Long]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getLong(column: String): Long? = getLong(indexFromColumn(column))

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [Float]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getFloat(column: String): Float? = getFloat(indexFromColumn(column))

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [LocalDate]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getLocalDate(column: String): LocalDate? = getLocalDate(indexFromColumn(column))

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [LocalTime]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getLocalTime(column: String): LocalTime? = getLocalTime(indexFromColumn(column))

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [LocalDateTime]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getLocalDateTime(column: String): LocalDateTime? {
    return getLocalDateTime(indexFromColumn(column))
}

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [DateTime]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getDateTime(column: String): DateTime? = getDateTime(indexFromColumn(column))

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [String]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getString(column: String): String? = getString(indexFromColumn(column))

fun <T> DataRow.getList(column: String): List<T?>? = getList(indexFromColumn(column))
