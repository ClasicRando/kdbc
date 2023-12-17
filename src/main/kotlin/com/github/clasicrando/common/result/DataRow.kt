package com.github.clasicrando.common.result

import com.github.clasicrando.common.datetime.DateTime
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime

/**
 * Representation of a [ResultSet] row, allowing for fetching of each field's value by index or
 * column name.
 */
interface DataRow {
    /**
     * Return the index of the specified [column] name. Throws an [IllegalArgumentException] if the
     * column is not available within the row.
     */
    fun indexFromColumn(column: String): Int
    /**
     * Get the value stored within the field at the [index] specified. Throws an
     * [IllegalArgumentException] if the index is out of range of the row.
     */
    operator fun get(index: Int): Any?
}

/**
 * Get the value stored within the field of [column] specified. Throws an
 * [IllegalArgumentException] if the [column] is not available within the row.
 */
operator fun DataRow.get(column: String): Any? = get(indexFromColumn(column))

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
 * Get the value stored within the field at the [index] specified, attempting to coerce the
 * underlining value to a [Boolean]. Throws an [IllegalArgumentException] if the index is out of
 * range of the row.
 */
fun DataRow.getBoolean(index: Int): Boolean? = getBooleanCoerce(get(index))

/**
 * Get the value stored within the field of [column] specified, attempting to coerce the
 * underlining value to a [Boolean]. Throws an [IllegalArgumentException] if the index is out of
 * range of the row.
 */
fun DataRow.getBoolean(column: String): Boolean? = getBooleanCoerce(get(column))

/**
 * Attempt to either cast the value as [Boolean] (if possible), otherwise, integer types are
 * coerced to a [Boolean] treating 1 as true and all other values as false. Throws an
 * [IllegalArgumentException] if [value] is not a [Boolean] or integer type.
 */
private fun getBooleanCoerce(value: Any?): Boolean? {
    return when (value) {
        null -> return null
        is Boolean -> value
        is Byte -> value == 1.toByte()
        is Short -> value == 1.toShort()
        is Int -> value == 1
        else -> error("Cannot coerce type to boolean. value -> '$value', type -> ${value::class} ")
    }
}

/**
 * Get the value stored within the field at the [index] specified, performing a non-safe cast to
 * [Byte]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getByte(index: Int): Byte? = getAs(index)

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [Byte]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getByte(column: String): Byte? = getAs(column)

/**
 * Get the value stored within the field at the [index] specified, performing a non-safe cast to
 * [Short]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getShort(index: Int): Short? = getAs(index)

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [Short]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getShort(column: String): Short? = getAs(column)

/**
 * Get the value stored within the field at the [index] specified, performing a non-safe cast to
 * [Int]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getInt(index: Int): Int? = getAs(index)

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [Int]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getInt(column: String): Int? = getAs(column)

/**
 * Get the value stored within the field at the [index] specified, performing a non-safe cast to
 * [Long]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getLong(index: Int): Long? = getAs(index)

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [Long]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getLong(column: String): Long? = getAs(column)

/**
 * Get the value stored within the field at the [index] specified, performing a non-safe cast to
 * [Float]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getFloat(index: Int): Float? = getAs(index)

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [Float]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getFloat(column: String): Float? = getAs(column)

/**
 * Get the value stored within the field at the [index] specified, performing a non-safe cast to
 * [LocalDate]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getLocalDate(index: Int): LocalDate? = getAs(index)

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [LocalDate]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getLocalDate(column: String): LocalDate? = getAs(column)

/**
 * Get the value stored within the field at the [index] specified, performing a non-safe cast to
 * [LocalTime]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getLocalTime(index: Int): LocalTime? = getAs(index)

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [LocalTime]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getLocalTime(column: String): LocalTime? = getAs(column)

/**
 * Get the value stored within the field at the [index] specified, performing a non-safe cast to
 * [LocalDateTime]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getLocalDateTime(index: Int): LocalDateTime? = getAs(index)

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [LocalDateTime]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getLocalDateTime(column: String): LocalDateTime? = getAs(column)

/**
 * Get the value stored within the field at the [index] specified, performing a non-safe cast to
 * [DateTime]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getDateTime(index: Int): DateTime? = getAs(index)

/**
 * Get the value stored within the field of [column] specified, performing a non-safe cast to
 * [DateTime]. Throws an [IllegalArgumentException] if the index is out of range of the row.
 */
fun DataRow.getDateTime(column: String): DateTime? = getAs(column)
