package com.github.kdbc.core.result

import com.github.kdbc.core.AutoRelease
import com.github.kdbc.core.datetime.DateTime
import com.github.kdbc.core.exceptions.IncorrectScalarType
import com.github.kdbc.core.exceptions.NoResultFound
import com.github.kdbc.core.exceptions.RowParseError
import com.github.kdbc.core.query.RowParser
import com.github.kdbc.core.use
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlin.reflect.KClass

/**
 * Container class for the data returned upon completion of a query. Every query must have the
 * number of rows affected, the message sent to the client and the rows returned (empty result if
 * no rows returned).
 *
 * This type is not thread safe and should be accessed by a single thread or coroutine to ensure
 * consistent processing of data.
 */
open class QueryResult(
    val rowsAffected: Long,
    val message: String,
    val rows: ResultSet = ResultSet.EMPTY_RESULT,
) : AutoRelease {
    override fun toString(): String {
        return "QueryResult(rowsAffected=$rowsAffected,message=$message)"
    }

    /** Releases all [rows] found within this result */
    override fun release() {
        rows.release()
    }

    /**
     * Execute the query and return the first row's first column as the type [T]. Returns null if
     * the return value is null or the query result has no rows.
     *
     * @throws IllegalStateException if the query has already been closed
     * @throws NoResultFound if the execution result yields no [QueryResult]
     * @throws IncorrectScalarType if the scalar value is not an instance of the type [T], this
     * checked by [KClass.isInstance] on the first value
     */
    inline fun <reified T : Any> extractScalar(): T? {
        val cls = T::class
        return rows.firstOrNull()?.use { row ->
            val value = when (cls) {
                BOOLEAN_CLASS -> row.getBoolean(FIRST_INDEX)
                BYTE_CLASS -> row.getByte(FIRST_INDEX)
                SHORT_CLASS -> row.getShort(FIRST_INDEX)
                INT_CLASS -> row.getInt(FIRST_INDEX)
                LONG_CLASS -> row.getLong(FIRST_INDEX)
                FLOAT_CLASS -> row.getFloat(FIRST_INDEX)
                DOUBLE_CLASS -> row.getDouble(FIRST_INDEX)
                LOCAL_DATE_CLASS -> row.getLocalDate(FIRST_INDEX)
                LOCAL_TIME_CLASS -> row.getLocalTime(FIRST_INDEX)
                LOCAL_DATE_TIME_CLASS -> row.getLocalDateTime(FIRST_INDEX)
                DATETIME_CLASS -> row.getDateTime(FIRST_INDEX)
                STRING_CLASS -> row.getString(FIRST_INDEX)
                else -> row[FIRST_INDEX]
            }
            if (!cls.isInstance(value)) {
                throw IncorrectScalarType(value, cls)
            }
            value as T?
        }
    }

    /**
     * Return the first row parsed as the type [T] by the supplied [rowParser]. Returns null if the
     * query results no rows.
     *
     * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
     * [RowParseError] are wrapped into a [RowParseError]
     */
    fun <T : Any, R : RowParser<T>> extractFirst(rowParser: R): T? {
        return rows.firstOrNull()
            ?.use { row ->
                try {
                    rowParser.fromRow(row)
                } catch (ex: RowParseError) {
                    throw ex
                } catch (ex: Throwable) {
                    throw RowParseError(rowParser, ex)
                }
            }
    }

    /**
     * Return the all rows as a [Sequence] where each row is parsed as the type [T] by the supplied
     * [rowParser]. Returns an empty [Sequence] when no rows are returned.
     *
     * @throws IllegalStateException if the query has already been closed
     * @throws NoResultFound if the execution result yields no [QueryResult]
     * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
     * [RowParseError] are wrapped into a [RowParseError]
     */
    fun <T : Any, R : RowParser<T>> extractAll(rowParser: R): Sequence<T> {
        return rows.asSequence()
            .map { row ->
                try {
                    rowParser.fromRow(row)
                } catch (ex: RowParseError) {
                    throw ex
                } catch (ex: Throwable) {
                    throw RowParseError(rowParser, ex)
                }
            }
    }

    companion object {
        @PublishedApi
        internal const val FIRST_INDEX = 0
        @PublishedApi
        internal val BOOLEAN_CLASS = Boolean::class
        @PublishedApi
        internal val BYTE_CLASS = Byte::class
        @PublishedApi
        internal val SHORT_CLASS = Short::class
        @PublishedApi
        internal val INT_CLASS = Int::class
        @PublishedApi
        internal val LONG_CLASS = Long::class
        @PublishedApi
        internal val FLOAT_CLASS = Float::class
        @PublishedApi
        internal val DOUBLE_CLASS = Double::class
        @PublishedApi
        internal val LOCAL_DATE_CLASS = LocalDate::class
        @PublishedApi
        internal val LOCAL_TIME_CLASS = LocalTime::class
        @PublishedApi
        internal val LOCAL_DATE_TIME_CLASS = LocalDateTime::class
        @PublishedApi
        internal val DATETIME_CLASS = DateTime::class
        @PublishedApi
        internal val STRING_CLASS = String::class
    }
}
