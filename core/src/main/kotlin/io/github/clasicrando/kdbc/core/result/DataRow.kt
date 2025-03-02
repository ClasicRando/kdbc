package io.github.clasicrando.kdbc.core.result

import io.github.clasicrando.kdbc.core.column.ColumnExtractError
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import kotlin.reflect.KType
import kotlin.reflect.typeOf

/**
 * Representation of a result row. Allows for fetching of each field's value by index or column
 * name. Fields can be read multiple times since implementors use the data backing each field in a
 * copy and reset type behaviour.
 *
 * This type is not thread safe and should be accessed by a single thread or coroutine to ensure
 * consistent processing of data.
 */
public interface DataRow {
    /**
     * Return the index of the specified [column] name.
     *
     * @throws IllegalArgumentException [column] name cannot be found in the row
     */
    public fun indexFromColumn(column: String): Int

    /**
     * Get the value stored within the field at the [index] specified. The specified [type] tell the
     * row decoder to look up the type description and attempt to decode into that type.
     *
     * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
     *   already been decoded
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be
     *   decoded to the specified [type]
     */
    public fun get(index: Int, type: KType): Any?

    /**
     * Get the value stored within the field of [column] specified. The specified [type] tell the
     * row decoder to look up the type description and attempt to decode into that type.
     *
     * @throws IllegalArgumentException if the [column] is not in the row or the field has already
     *   been decoded
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be
     *   decoded to the specified [type]
     */
    public fun get(column: String, type: KType): Any? = get(indexFromColumn(column), type)

    /**
     * Get the value stored within the field at the [index] specified as a [Boolean]. Returns null
     * if the SQL value is null.
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be
     *   decoded to a [Boolean]
     */
    public fun getBoolean(index: Int): Boolean?

    /**
     * Get the value stored within the field at the [index] specified as a [Byte]. Returns null if
     * the SQL value is null.
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be
     *   decoded to a [Byte]
     */
    public fun getByte(index: Int): Byte?

    /**
     * Get the value stored within the field at the [index] specified as a [Short]. Returns null if
     * the SQL value is null.
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be
     *   decoded to a [Short]
     */
    public fun getShort(index: Int): Short?

    /**
     * Get the value stored within the field at the [index] specified as a [Int]. Returns null if
     * the SQL value is null.
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be
     *   decoded to a [Int]
     */
    public fun getInt(index: Int): Int?

    /**
     * Get the value stored within the field at the [index] specified as a [Long]. Returns null if
     * the SQL value is null.
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be
     *   decoded to a [Long]
     */
    public fun getLong(index: Int): Long?

    /**
     * Get the value stored within the field at the [index] specified as a [Float]. Returns null if
     * the SQL value is null.
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be
     *   decoded to a [Float]
     */
    public fun getFloat(index: Int): Float?

    /**
     * Get the value stored within the field at the [index] specified as a [Double]. Returns null if
     * the SQL value is null.
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be
     *   decoded to a [Double]
     */
    public fun getDouble(index: Int): Double?

    /**
     * Get the value stored within the field at the [index] specified as a [LocalTime]. Returns null
     * if the SQL value is null.
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be
     *   decoded to a [LocalTime]
     */
    public fun getLocalTime(index: Int): LocalTime?

    /**
     * Get the value stored within the field at the [index] specified as a [LocalDate]. Returns null
     * if the SQL value is null.
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be
     *   decoded to a [LocalDate]
     */
    public fun getLocalDate(index: Int): LocalDate?

    /**
     * Get the value stored within the field at the [index] specified as a [LocalDateTime]. Returns
     * null if the SQL value is null.
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be
     *   decoded to a [LocalDateTime]
     */
    public fun getLocalDateTime(index: Int): LocalDateTime?

    /**
     * Get the value stored within the field at the [index] specified as a [Instant]. Returns null
     * if the SQL value is null.
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be
     *   decoded to a [Instant]
     */
    public fun getInstant(index: Int): Instant?

    /**
     * Get the value stored within the field at the [index] specified as a [OffsetDateTime]. Returns
     * null if the SQL value is null.
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be
     *   decoded to a [OffsetDateTime]
     */
    public fun getOffsetDateTime(index: Int): OffsetDateTime?

    /**
     * Get the value stored within the field at the [index] specified as a [BigDecimal]. Returns
     * null if the SQL value is null.
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be
     *   decoded to a [BigDecimal]
     */
    public fun getBigDecimal(index: Int): BigDecimal?

    /**
     * Get the value stored within the field at the [index] specified as a [ByteArray]. Returns null
     * if the SQL value is null.
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be
     *   decoded to a [ByteArray]
     */
    public fun getBytes(index: Int): ByteArray?

    /**
     * Get the value stored within the field at the [index] specified as a [String]. Returns null if
     * the SQL value is null.
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be
     *   decoded to a [String]
     */
    public fun getString(index: Int): String?
}

/**
 * Get the value stored within the field at the [index] specified and return the value if it matches
 * the type [T] required. If the type cannot be successfully cast, an exception is thrown. If the
 * value is null, null is returned without checking types.
 *
 * @throws IllegalArgumentException if the [index] is out of range of the row or the field has
 *   already been decoded
 * @throws ColumnExtractError if the column value cannot be cast to the desired type [T]
 */
public inline fun <reified T : Any> DataRow.getAs(index: Int): T? {
    val value = get(index, typeOf<T>()) ?: return null
    if (value is T) {
        return value
    }
    throw ColumnExtractError(typeOf<T>(), value)
}

/**
 * Get the value stored within the field at the [index] specified and return the value if it matches
 * the type [T] required. If the type cannot be successfully cast, an exception is thrown. If the
 * value is null, null is returned without checking types.
 *
 * @throws IllegalArgumentException if the [index] is out of range of the row
 * @throws ColumnExtractError if the column value cannot be cast to the desired type [T]
 * @throws NullPointerException if the column value is null
 */
public inline fun <reified T : Any> DataRow.getAsNonNull(index: Int): T {
    return getAs(index) ?: throw KdbcException("Expected non-null field value but got null")
}

/**
 * Get the value stored within the field at the [column] specified and return the value if it
 * matches the type [T] required. If the type cannot be successfully cast, an exception is thrown.
 * If the value is null, null is returned without checking types.
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row
 * @throws ColumnExtractError if the column value cannot be cast to the desired type [T]
 */
public inline fun <reified T : Any> DataRow.getAs(column: String): T? {
    return getAs(indexFromColumn(column))
}

/**
 * Get the value stored within the field at the [column] specified and return the value if it
 * matches the type [T] required. If the type cannot be successfully cast, an exception is thrown.
 * If the value is null, null is returned without checking types.
 *
 * @throws IllegalArgumentException if the [column] is out of range of the row
 * @throws ColumnExtractError if the column value cannot be cast to the desired type [T]
 */
public inline fun <reified T : Any> DataRow.getAsNonNull(column: String): T {
    return getAsNonNull(indexFromColumn(column))
}

/**
 * Get the value stored within the field at the [index] specified as a [Boolean].
 *
 * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be decoded
 *   to a [Boolean]
 * @throws KdbcException if the field value is null
 */
public fun DataRow.getBooleanNonNull(index: Int): Boolean {
    return getBoolean(index) ?: throw KdbcException("Expected non-null field value but got null")
}

/**
 * Get the value stored within the field at the [index] specified as a [Byte].
 *
 * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be decoded
 *   to a [Byte]
 * @throws KdbcException if the field value is null
 */
public fun DataRow.getByteNonNull(index: Int): Byte {
    return getByte(index) ?: throw KdbcException("Expected non-null field value but got null")
}

/**
 * Get the value stored within the field at the [index] specified as a [Short].
 *
 * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be decoded
 *   to a [Short]
 * @throws KdbcException if the field value is null
 */
public fun DataRow.getShortNonNull(index: Int): Short {
    return getShort(index) ?: throw KdbcException("Expected non-null field value but got null")
}

/**
 * Get the value stored within the field at the [index] specified as a [Int].
 *
 * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be decoded
 *   to a [Int]
 * @throws KdbcException if the field value is null
 */
public fun DataRow.getIntNonNull(index: Int): Int {
    return getInt(index) ?: throw KdbcException("Expected non-null field value but got null")
}

/**
 * Get the value stored within the field at the [index] specified as a [Long].
 *
 * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be decoded
 *   to a [Long]
 * @throws KdbcException if the field value is null
 */
public fun DataRow.getLongNonNull(index: Int): Long {
    return getLong(index) ?: throw KdbcException("Expected non-null field value but got null")
}

/**
 * Get the value stored within the field at the [index] specified as a [Float].
 *
 * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be decoded
 *   to a [Float]
 * @throws KdbcException if the field value is null
 */
public fun DataRow.getFloatNonNull(index: Int): Float {
    return getFloat(index) ?: throw KdbcException("Expected non-null field value but got null")
}

/**
 * Get the value stored within the field at the [index] specified as a [Double].
 *
 * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be decoded
 *   to a [Double]
 * @throws KdbcException if the field value is null
 */
public fun DataRow.getDoubleNonNull(index: Int): Double {
    return getDouble(index) ?: throw KdbcException("Expected non-null field value but got null")
}

/**
 * Get the value stored within the field at the [index] specified as a [LocalTime].
 *
 * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be decoded
 *   to a [LocalTime]
 * @throws KdbcException if the field value is null
 */
public fun DataRow.getLocalTimeNonNull(index: Int): LocalTime {
    return getLocalTime(index) ?: throw KdbcException("Expected non-null field value but got null")
}

/**
 * Get the value stored within the field at the [index] specified as a [LocalDate].
 *
 * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be decoded
 *   to a [LocalDate]
 * @throws KdbcException if the field value is null
 */
public fun DataRow.getLocalDateNonNull(index: Int): LocalDate {
    return getLocalDate(index) ?: throw KdbcException("Expected non-null field value but got null")
}

/**
 * Get the value stored within the field at the [index] specified as a [LocalDateTime].
 *
 * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be decoded
 *   to a [LocalDateTime]
 * @throws KdbcException if the field value is null
 */
public fun DataRow.getLocalDateTimeNonNull(index: Int): LocalDateTime {
    return getLocalDateTime(index)
        ?: throw KdbcException("Expected non-null field value but got null")
}

/**
 * Get the value stored within the field at the [index] specified as a [Instant].
 *
 * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be decoded
 *   to a [Instant]
 * @throws KdbcException if the field value is null
 */
public fun DataRow.getInstantNonNull(index: Int): Instant {
    return getInstant(index) ?: throw KdbcException("Expected non-null field value but got null")
}

/**
 * Get the value stored within the field at the [index] specified as a [OffsetDateTime].
 *
 * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be decoded
 *   to a [OffsetDateTime]
 * @throws KdbcException if the field value is null
 */
public fun DataRow.getOffsetDateTimeNonNull(index: Int): OffsetDateTime {
    return getOffsetDateTime(index)
        ?: throw KdbcException("Expected non-null field value but got null")
}

/**
 * Get the value stored within the field at the [index] specified as a [BigDecimal].
 *
 * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be decoded
 *   to a [BigDecimal]
 * @throws KdbcException if the field value is null
 */
public fun DataRow.getBigDecimalNonNull(index: Int): BigDecimal {
    return getBigDecimal(index) ?: throw KdbcException("Expected non-null field value but got null")
}

/**
 * Get the value stored within the field at the [index] specified as a [ByteArray].
 *
 * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be decoded
 *   to a [ByteArray]
 * @throws KdbcException if the field value is null
 */
public fun DataRow.getBytesNonNull(index: Int): ByteArray {
    return getBytes(index) ?: throw KdbcException("Expected non-null field value but got null")
}

/**
 * Get the value stored within the field at the [index] specified as a [String].
 *
 * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the column cannot be decoded
 *   to a [String]
 * @throws KdbcException if the field value is null
 */
public fun DataRow.getStringNonNull(index: Int): String {
    return getString(index) ?: throw KdbcException("Expected non-null field value but got null")
}
