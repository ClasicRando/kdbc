package io.github.clasicrando.kdbc.core.result

import io.github.clasicrando.kdbc.core.exceptions.IncorrectScalarType
import io.github.clasicrando.kdbc.core.exceptions.RowParseError
import io.github.clasicrando.kdbc.core.query.RowParser
import kotlin.reflect.typeOf

/**
 * Container class for the data returned upon completion of a query. Every query must have the
 * number of rows affected, the message sent to the client and the rows returned (empty result if no
 * rows returned).
 *
 * This type is not thread safe and should be accessed by a single thread or coroutine to ensure
 * consistent processing of data.
 */
public open class QueryResult(
    public val rowsAffected: Long,
    public val message: String,
    public val rows: ResultSet = ResultSet.EMPTY_RESULT,
) {
    /**
     * Execute the query and return the first row's first column as the type [T]. Returns null if
     * the return value is null or the query result has no rows.
     *
     * @throws IllegalStateException if the query has already been closed
     * @throws io.github.clasicrando.kdbc.core.exceptions.NoResultFound if the execution result
     *   yields no [QueryResult]
     * @throws IncorrectScalarType if the scalar value is not an instance of the type [T], this
     *   checked by [kotlin.reflect.KClass.isInstance] on the first value
     */
    public inline fun <reified T : Any> extractScalar(): T? {
        if (rows.rowCount == 0) {
            return null
        }
        val value = rows[0][FIRST_INDEX, typeOf<T>()] ?: return null
        return value as T?
    }

    /**
     * Return the first row parsed as the type [T] by the supplied [rowParser]. Returns null if the
     * query results no rows.
     *
     * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
     *   [RowParseError] are wrapped into a [RowParseError]
     */
    public fun <T : Any, R : RowParser<T>> extractFirst(rowParser: R): T? {
        if (rows.rowCount == 0) {
            return null
        }
        return try {
            rowParser.fromRow(rows[0])
        } catch (ex: RowParseError) {
            throw ex
        } catch (ex: Exception) {
            throw RowParseError(rowParser, ex)
        }
    }

    /**
     * Return the all rows as a [List] where each row is parsed as the type [T] by the supplied
     * [rowParser]. Returns an empty [List] when no rows are returned.
     *
     * @throws IllegalStateException if the query has already been closed
     * @throws io.github.clasicrando.kdbc.core.exceptions.NoResultFound if the execution result
     *   yields no [QueryResult]
     * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
     *   [RowParseError] are wrapped into a [RowParseError]
     */
    public fun <T : Any, R : RowParser<T>> extractAll(rowParser: R): List<T> {
        val result = mutableListOf<T>()
        for (i in 0..<rows.rowCount) {
            try {
                result.add(rowParser.fromRow(rows[i]))
            } catch (ex: RowParseError) {
                throw ex
            } catch (ex: Exception) {
                throw RowParseError(rowParser, ex)
            }
        }
        return result
    }

    override fun toString(): String = "QueryResult(rowsAffected=$rowsAffected,message=$message)"

    public companion object {
        @PublishedApi internal const val FIRST_INDEX: Int = 0
    }
}
