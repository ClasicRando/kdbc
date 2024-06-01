package io.github.clasicrando.kdbc.core.result

import io.github.clasicrando.kdbc.core.AutoRelease
import io.github.clasicrando.kdbc.core.exceptions.IncorrectScalarType
import io.github.clasicrando.kdbc.core.exceptions.RowParseError
import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.use

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
     * @throws io.github.clasicrando.kdbc.core.exceptions.NoResultFound if the execution result
     * yields no [QueryResult]
     * @throws IncorrectScalarType if the scalar value is not an instance of the type [T], this
     * checked by [KClass.isInstance] on the first value
     */
    inline fun <reified T : Any> extractScalar(): T? {
        val cls = T::class
        return rows.firstOrNull()?.use { row ->
            val value = row[FIRST_INDEX]
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
     * Return the all rows as a [List] where each row is parsed as the type [T] by the supplied
     * [rowParser]. Returns an empty [List] when no rows are returned.
     *
     * @throws IllegalStateException if the query has already been closed
     * @throws io.github.clasicrando.kdbc.core.exceptions.NoResultFound if the execution result
     * yields no [QueryResult]
     * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
     * [RowParseError] are wrapped into a [RowParseError]
     */
    fun <T : Any, R : RowParser<T>> extractAll(rowParser: R): List<T> {
        return rows.map { row ->
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
    }
}
