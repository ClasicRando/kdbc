package io.github.clasicrando.kdbc.core.result

import io.github.clasicrando.kdbc.core.AutoRelease
import io.github.clasicrando.kdbc.core.column.ColumnData
import io.github.clasicrando.kdbc.core.column.ColumnExtractError
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
     * the type as specified by the [ColumnData] and you must know that and check/cast
     * appropriately.
     *
     * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
     * already been decoded
     */
    operator fun get(index: Int): Any?

    /**
     * Get the value stored within the field of [column] specified. This will always decode to
     * the type as specified by the [ColumnData] and you must know that and check/cast
     * appropriately.
     *
     * @throws IllegalArgumentException if the [column] is not in the row or the field has already
     * been decoded
     */
    operator fun get(column: String): Any? = get(indexFromColumn(column))
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
    throw ColumnExtractError(typeOf<T>(), value, value::class)
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
inline fun <reified T : Any> DataRow.getAsNonNull(index: Int): T {
    return getAs(index) ?: throw NullPointerException("Expected non-null field value but got null")
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
 * Get the value stored within the field at the [column] specified and return the value if it
 * matches the type [T] required. If the type cannot be successfully cast, an exception is thrown.
 * If the value is null, this will never fail but just return null.
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row or the field has
 * already been decoded
 * @throws ColumnExtractError if the column value cannot be cast to the desired type [T]
 */
inline fun <reified T : Any> DataRow.getAsNonNull(column: String): T {
    return getAsNonNull(indexFromColumn(column))
}
