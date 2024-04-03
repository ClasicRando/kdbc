package com.github.clasicrando.common.query

import com.github.clasicrando.common.AutoRelease
import com.github.clasicrando.common.connection.Connection
import com.github.clasicrando.common.datetime.DateTime
import com.github.clasicrando.common.exceptions.EmptyQueryResult
import com.github.clasicrando.common.exceptions.IncorrectScalarType
import com.github.clasicrando.common.exceptions.NoResultFound
import com.github.clasicrando.common.exceptions.RowParseError
import com.github.clasicrando.common.exceptions.TooManyRows
import com.github.clasicrando.common.result.QueryResult
import com.github.clasicrando.common.result.StatementResult
import com.github.clasicrando.common.use
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlin.reflect.KClass

/**
 * API to perform queries without interfacing with the more complex [Connection.sendQuery] and
 * [Connection.sendPreparedStatement] methods. This allows the user to leverage a [Connection],
 * optionally bind parameters to the [Query] and then execute the query, fetching the results in
 * common ways with helper methods. You can also reuse the query with new parameters by calling
 * [clearParameters] between any of the `fetch` method calls to provide different parameters.
 */
open class Query(
    /** SQL Query to be executed against the database */
    val query: String,
    /** Reference to the [Connection] that backs this [Query] */
    @PublishedApi internal var connection: Connection?,
) : AutoRelease {
    /** Internal list of parameters that are bound to this [Query] */
    @PublishedApi
    internal val parameters: MutableList<Any?> = mutableListOf()
    /** Read-only [List] of the parameters currently bound to this [Query] */
    val params: List<Any?> get() = parameters

    /**
     * Bind a next [parameter] to the [Query]. This adds the parameter to the internal list of
     * parameters in the order the parameter exists in the query regardless of the vendor specific
     * method of linking parameter values to query parameters.
     *
     * Returns a reference to the [Query] to allow for method chaining.
     */
    fun bind(parameter: Any?): Query {
        parameters += parameter
        return this
    }

    /**
     * Bind the [parameters] to the [Query]. This adds each parameter to the internal list of
     * parameters in the order the parameter exists in the query regardless of the vendor specific
     * method of linking parameter values to query parameters.
     *
     * Returns a reference to the [Query] to allow for method chaining.
     */
    fun bindMany(parameters: Collection<Any?>): Query {
        this.parameters.addAll(parameters)
        return this
    }

    /**
     * Bind the [parameters] to the [Query]. This adds each parameter to the internal list of
     * parameters in the order the parameter exists in the query regardless of the vendor specific
     * method of linking parameter values to query parameters.
     *
     * Returns a reference to the [Query] to allow for method chaining.
     */
    fun bindMany(vararg parameters: Any?): Query {
        this.parameters.addAll(parameters)
        return this
    }

    /** Clears all parameters previously bound to this [Query]  */
    fun clearParameters() {
        parameters.clear()
    }

    /**
     * Simply execute the query and return the raw [StatementResult]. Use this if you have a
     * non-SELECT DML statement to execute (e.g. an INSERT query where there is no result).
     *
     * @throws IllegalStateException if the query has already been closed
     */
    suspend fun execute(): StatementResult {
        checkNotNull(connection) { "Query already released its Connection" }
        return connection!!.sendPreparedStatement(query, parameters)
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
    suspend inline fun <reified T : Any> fetchScalar(): T? {
        checkNotNull(connection) { "Query already released its Connection" }
        val cls = T::class
        return connection!!.sendPreparedStatement(query, parameters).use { statementResult ->
            if (statementResult.size == 0) {
                throw NoResultFound(query)
            }
            statementResult.first().use { queryResult ->
                queryResult.rows.firstOrNull()?.use { row ->
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
        }
    }

    /**
     * Execute the query and return the first row parsed as the type [T] by the supplied
     * [rowParser]. Returns null if the query results no rows.
     *
     * @throws IllegalStateException if the query has already been closed
     * @throws NoResultFound if the execution result yields no [QueryResult]
     * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
     * [RowParseError] are wrapped into a [RowParseError]
     */
    suspend fun <T : Any, R : RowParser<T>> fetchFirst(rowParser: R): T? {
        checkNotNull(connection) { "Query already released its Connection" }
        return connection!!.sendPreparedStatement(query, parameters).use { statementResult ->
            if (statementResult.size == 0) {
                throw NoResultFound(query)
            }
            statementResult.first().use { queryResult ->
                queryResult.rows.firstOrNull()?.use { row ->
                    try {
                        rowParser.fromRow(row)
                    } catch (ex: RowParseError) {
                        throw ex
                    } catch (ex: Throwable) {
                        throw RowParseError(rowParser, ex)
                    }
                }
            }
        }
    }

    /**
     * Execute the query and return the first row parsed as the type [T] by the supplied
     * [rowParser].
     *
     * @throws IllegalStateException if the query has already been closed
     * @throws NoResultFound if the execution result yields no [QueryResult]
     * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
     * [RowParseError] are wrapped into a [RowParseError]
     * @throws EmptyQueryResult if the query returns no rows
     * @throws TooManyRows if the [QueryResult.rowsAffected] value > 1
     */
    suspend fun <T : Any, R : RowParser<T>> fetchSingle(rowParser: R): T {
        checkNotNull(connection) { "Query already released its Connection" }
        return connection!!.sendPreparedStatement(query, parameters).use { statementResult ->
            if (statementResult.size == 0) {
                throw NoResultFound(query)
            }
            statementResult.first().use { queryResult ->
                if (queryResult.rowsAffected > 1) {
                    throw TooManyRows(query)
                }
                queryResult.rows.firstOrNull()?.use { row ->
                    try {
                        rowParser.fromRow(row)
                    } catch (ex: RowParseError) {
                        throw ex
                    } catch (ex: Throwable) {
                        throw RowParseError(rowParser, ex)
                    }
                } ?: throw EmptyQueryResult(query)
            }
        }
    }

    /**
     * Execute the query and return the all rows in a [List] where each row is parsed as the type
     * [T] by the supplied [rowParser]. Returns an empty [List] when no rows are returned.
     *
     * @throws IllegalStateException if the query has already been closed
     * @throws NoResultFound if the execution result yields no [QueryResult]
     * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
     * [RowParseError] are wrapped into a [RowParseError]
     */
    suspend fun <T : Any, R : RowParser<T>> fetchAll(rowParser: R): List<T> {
        checkNotNull(connection) { "Query already released its Connection" }
        return connection!!.sendPreparedStatement(query, parameters).use { statementResult ->
            if (statementResult.size == 0) {
                throw NoResultFound(query)
            }
            statementResult.first().use { queryResult ->
                queryResult.rows.map { row ->
                    try {
                        rowParser.fromRow(row)
                    } catch (ex: RowParseError) {
                        throw ex
                    } catch (ex: Throwable) {
                        throw RowParseError(rowParser, ex)
                    }
                }
            }
        }
    }

    /**
     * Execute the query and return the all rows as a [Flow] where each row is parsed as the type
     * [T] by the supplied [rowParser]. Resulting [Flow] is cold so the connection is still in use
     * until every row is collected or the [Flow] is canceled.
     *
     * @throws IllegalStateException if the query has already been closed
     * @throws NoResultFound if the execution result yields no [QueryResult]
     * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
     * [RowParseError] are wrapped into a [RowParseError]
     */
    suspend fun <T : Any, R : RowParser<T>> fetch(rowParser: R): Flow<T> = flow {
        checkNotNull(connection) { "Query already released its Connection" }
        connection!!.sendPreparedStatement(query, parameters).use { statementResult ->
            if (statementResult.size == 0) {
                throw NoResultFound(query)
            }
            statementResult.first().use { queryResult ->
                for (row in queryResult.rows) {
                    try {
                        emit(rowParser.fromRow(row))
                    } catch (ex: RowParseError) {
                        throw ex
                    } catch (ex: Throwable) {
                        throw RowParseError(rowParser, ex)
                    }
                }
            }
        }
    }

    override fun release() {
        connection = null
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
