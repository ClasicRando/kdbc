package com.github.clasicrando.common.result

import com.github.clasicrando.common.AutoRelease
import com.github.clasicrando.common.column.ColumnDecodeError
import com.github.clasicrando.common.column.ColumnExtractError
import com.github.clasicrando.common.datetime.DateTime
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset
import kotlin.reflect.typeOf

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
     * Get the value stored within the field at the [index] specified. This will always decode to
     * the type as specified by the [ColumnData][com.github.clasicrando.common.column.ColumnData]
     * and you must know that and check/cast appropriately.
     *
     * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
     * already been decoded
     */
    operator fun get(index: Int): Any?

    /**
     * Get the value stored within the field of [column] specified. This will always decode to
     * the type as specified by the [ColumnData][com.github.clasicrando.common.column.ColumnData]
     * and you must know that and check/cast appropriately.
     *
     * @throws IllegalArgumentException if the [column] is not in the row or the field has already
     * been decoded
     */
    operator fun get(column: String): Any? = get(indexFromColumn(column))

    /**
     * Get the value stored within the field at the [index] specified, attempting to coerce the
     * underlining value to a [Boolean].
     *
     * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getBoolean(index: Int): Boolean?

    /**
     * Get the value stored within the field at the [index] specified as a [Byte]. If the actual
     * column type can be safely coerced to [Byte] this operation will not fail and will return
     * that type as a [Byte].
     *
     * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getByte(index: Int): Byte?

    /**
     * Get the value stored within the field at the [index] specified as a [Short]. If the actual
     * column type can be safely coerced to [Short] this operation will not fail and will return
     * that type as a [Short].
     *
     * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getShort(index: Int): Short?

    /**
     * Get the value stored within the field at the [index] specified as an [Int]. If the actual
     * column type can be safely coerced to [Int] this operation will not fail and will return that
     * type as a [Int].
     *
     * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getInt(index: Int): Int?

    /**
     * Get the value stored within the field at the [index] specified as a [Long]. If the actual
     * column type can be safely coerced to [Long] this operation will not fail and will return
     * that type as a [Long].
     *
     * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getLong(index: Int): Long?

    /**
     * Get the value stored within the field at the [index] specified as a [Float].
     *
     * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact type, or the
     * decode operation failed
     */
    fun getFloat(index: Int): Float?

    /**
     * Get the value stored within the field at the [index] specified as a [Double]. If the actual
     * column type can be safely coerced to [Double] this operation will not fail and will return
     * that type as a [Double].
     *
     * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getDouble(index: Int): Double?

    /**
     * Get the value stored within the field at the [index] specified as a [LocalDate].
     *
     * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getLocalDate(index: Int): LocalDate?

    /**
     * Get the value stored within the field at the [index] specified as a [LocalTime].
     *
     * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getLocalTime(index: Int): LocalTime?

    /**
     * Get the value stored within the field at the [index] specified as a [LocalDateTime].
     *
     * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getLocalDateTime(index: Int): LocalDateTime?

    /**
     * Get the value stored within the field at the [index] specified as a [DateTime].
     *
     * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getDateTime(index: Int): DateTime?

    /**
     * Get the value stored within the field at the [index] specified as a [DateTime] with the
     * desired [offset] applied to the value returned from the database.
     *
     * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getDateTime(index: Int, offset: UtcOffset): DateTime?

    /**
     * Get the value stored within the field at the [index] specified as a [String].
     *
     * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     */
    fun getString(index: Int): String?

    /**
     * Get the value stored within the field at the [index] specified as a [List] of the desired
     * type [T]. If the database does not support querying data as a list or collection, this
     * method will always throw an exception.
     *
     * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
     * already been decoded
     * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
     * type, or the decode operation failed
     * @throws NotImplementedError if the database does not support querying data as a list or
     * collection
     */
    fun <T> getList(index: Int): List<T?>?
}

/**
 * Get the value stored within the field at the [index] specified and return the value if it
 * matches the type [T] required. If the type cannot be successfully cast, an exception is thrown.
 * If the value is null, this will never fail but just return null.
 *
 * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
 * already been decoded
 * @throws ColumnExtractError if the column value cannot be cast to the desired type [T]
 */
inline fun <reified T : Any> DataRow.getAs(index: Int): T? {
    val value = get(index) ?: return null
    if (value is T) {
        return value
    }
    throw ColumnExtractError(typeOf<T>(), value)
}

/**
 * Get the value stored within the field at the [column] specified and return the value if it
 * matches the type [T] required. If the type cannot be successfully cast, an exception is thrown.
 * If the value is null, this will never fail but just return null.
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row or the field has
 * already been decoded
 * @throws ColumnExtractError if the column value cannot be cast to the desired type [T]
 */
inline fun <reified T : Any> DataRow.getAs(column: String): T? = getAs(indexFromColumn(column))

/**
 * Get the value stored within the field at the [column] specified, attempting to coerce the
 * underlining value to a [Boolean].
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row or the field has
 * already been decoded
 * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
 * type, or the decode operation failed
 */
fun DataRow.getBoolean(column: String): Boolean? = getBoolean(indexFromColumn(column))

/**
 * Get the value stored within the field at the [column] specified as a [Byte]. If the actual
 * column type can be safely coerced to [Byte] this operation will not fail and will return
 * that type as a [Byte].
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row or the field has
 * already been decoded
 * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
 * type, or the decode operation failed
 */
fun DataRow.getByte(column: String): Byte? = getByte(indexFromColumn(column))

/**
 * Get the value stored within the field at the [column] specified as a [Short]. If the actual
 * column type can be safely coerced to [Short] this operation will not fail and will return
 * that type as a [Short].
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row or the field has
 * already been decoded
 * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
 * type, or the decode operation failed
 */
fun DataRow.getShort(column: String): Short? = getShort(indexFromColumn(column))

/**
 * Get the value stored within the field at the [column] specified as an [Int]. If the actual
 * column type can be safely coerced to [Int] this operation will not fail and will return that
 * type as a [Int].
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row or the field has
 * already been decoded
 * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
 * type, or the decode operation failed
 */
fun DataRow.getInt(column: String): Int? = getInt(indexFromColumn(column))

/**
 * Get the value stored within the field at the [column] specified as a [Long]. If the actual
 * column type can be safely coerced to [Long] this operation will not fail and will return
 * that type as a [Long].
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row or the field has
 * already been decoded
 * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
 * type, or the decode operation failed
 */
fun DataRow.getLong(column: String): Long? = getLong(indexFromColumn(column))

/**
 * Get the value stored within the field at the [column] specified as a [Float].
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row or the field has
 * already been decoded
 * @throws ColumnDecodeError if the value within the column is not the exact type, or the decode
 * operation failed
 */
fun DataRow.getFloat(column: String): Float? = getFloat(indexFromColumn(column))

/**
 * Get the value stored within the field at the [column] specified as a [Double]. If the actual
 * column type can be safely coerced to [Double] this operation will not fail and will return
 * that type as a [Double].
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row or the field has
 * already been decoded
 * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
 * type, or the decode operation failed
 */
fun DataRow.getDouble(column: String): Double? = getDouble(indexFromColumn(column))

/**
 * Get the value stored within the field at the [column] specified as a [LocalDate].
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row or the field has
 * already been decoded
 * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
 * type, or the decode operation failed
 */
fun DataRow.getLocalDate(column: String): LocalDate? = getLocalDate(indexFromColumn(column))

/**
 * Get the value stored within the field at the [column] specified as a [LocalTime].
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row or the field has
 * already been decoded
 * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
 * type, or the decode operation failed
 */
fun DataRow.getLocalTime(column: String): LocalTime? = getLocalTime(indexFromColumn(column))

/**
 * Get the value stored within the field at the [column] specified as a [LocalDateTime].
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row or the field has
 * already been decoded
 * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
 * type, or the decode operation failed
 */
fun DataRow.getLocalDateTime(column: String): LocalDateTime? {
    return getLocalDateTime(indexFromColumn(column))
}

/**
 * Get the value stored within the field at the [column] specified as a [DateTime].
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row or the field has
 * already been decoded
 * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
 * type, or the decode operation failed
 */
fun DataRow.getDateTime(column: String): DateTime? {
    return getDateTime(indexFromColumn(column), UtcOffset(seconds = 0))
}

/**
 * Get the value stored within the field at the [column] specified as a [DateTime] with the
 * desired [offset] applied to the value returned from the database.
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row or the field has
 * already been decoded
 * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
 * type, or the decode operation failed
 */
fun DataRow.getDateTime(column: String, offset: UtcOffset): DateTime? {
    return getDateTime(indexFromColumn(column), offset)
}

/**
 * Get the value stored within the field at the [column] specified as a [String].
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row or the field has
 * already been decoded
 * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
 * type, or the decode operation failed
 */
fun DataRow.getString(column: String): String? = getString(indexFromColumn(column))

/**
 * Get the value stored within the field at the [column] specified as a [List] of the desired
 * type [T]. If the database does not support querying data as a list or collection, this
 * method will always throw an exception.
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row or the field has
 * already been decoded
 * @throws ColumnDecodeError if the value within the column is not the exact or a compatible
 * type, or the decode operation failed
 * @throws NotImplementedError if the database does not support querying data as a list or
 * collection
 */
fun <T> DataRow.getList(column: String): List<T?>? = getList(indexFromColumn(column))
