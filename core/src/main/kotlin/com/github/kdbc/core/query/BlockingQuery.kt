package com.github.kdbc.core.query

import com.github.kdbc.core.AutoRelease
import com.github.kdbc.core.connection.BlockingConnection
import com.github.kdbc.core.exceptions.EmptyQueryResult
import com.github.kdbc.core.exceptions.IncorrectScalarType
import com.github.kdbc.core.exceptions.NoResultFound
import com.github.kdbc.core.exceptions.RowParseError
import com.github.kdbc.core.exceptions.TooManyRows
import com.github.kdbc.core.result.QueryResult
import com.github.kdbc.core.result.StatementResult
import com.github.kdbc.core.use
import kotlin.reflect.KClass

/**
 * API to perform queries without interfacing with the more complex [BlockingConnection.sendQuery]
 * and [BlockingConnection.sendPreparedStatement] methods. This allows the user to leverage a
 * [BlockingConnection], optionally bind parameters to the [BlockingQuery] and then execute the
 * query, fetching the results in common ways with helper methods. You can also reuse the query
 * with new parameters by calling [clearParameters] between any of the `fetch` method calls to
 * provide different parameters.
 */
open class BlockingQuery(
    /** SQL Query to be executed against the database */
    val query: String,
    /** Reference to the [BlockingConnection] that backs this [BlockingQuery] */
    @PublishedApi internal var connection: BlockingConnection?,
) : AutoRelease {
    /** Internal list of parameters that are bound to this [BlockingQuery] */
    @PublishedApi
    internal val parameters: MutableList<Any?> = mutableListOf()

    /**
     * Bind a next [parameter] to the [BlockingQuery]. This adds the parameter to the internal list of
     * parameters in the order the parameter exists in the query regardless of the vendor specific
     * method of linking parameter values to query parameters.
     *
     * Returns a reference to the [BlockingQuery] to allow for method chaining.
     */
    fun bind(parameter: Any?): BlockingQuery {
        parameters += parameter
        return this
    }

    /**
     * Bind the [parameters] to the [BlockingQuery]. This adds each parameter to the internal list of
     * parameters in the order the parameter exists in the query regardless of the vendor specific
     * method of linking parameter values to query parameters.
     *
     * Returns a reference to the [BlockingQuery] to allow for method chaining.
     */
    fun bindMany(parameters: Collection<Any?>): BlockingQuery {
        this.parameters.addAll(parameters)
        return this
    }

    /**
     * Bind the [parameters] to the [BlockingQuery]. This adds each parameter to the internal list of
     * parameters in the order the parameter exists in the query regardless of the vendor specific
     * method of linking parameter values to query parameters.
     *
     * Returns a reference to the [BlockingQuery] to allow for method chaining.
     */
    fun bindMany(vararg parameters: Any?): BlockingQuery {
        this.parameters.addAll(parameters)
        return this
    }

    /** Clears all parameters previously bound to this [BlockingQuery]  */
    fun clearParameters() {
        parameters.clear()
    }

    /**
     * Simply execute the query and return the raw [StatementResult]. Use this if you have a
     * non-SELECT DML statement to execute (e.g. an INSERT query where there is no result).
     *
     * @throws IllegalStateException if the query has already been closed
     */
    fun execute(): StatementResult {
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
    inline fun <reified T : Any> fetchScalar(): T? {
        checkNotNull(connection) { "Query already released its Connection" }
        return connection!!.sendPreparedStatement(query, parameters).use { statementResult ->
            if (statementResult.size == 0) {
                throw NoResultFound(query)
            }
            statementResult.first()
                .use { queryResult -> queryResult.extractScalar() }
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
    fun <T : Any, R : RowParser<T>> fetchFirst(rowParser: R): T? {
        checkNotNull(connection) { "Query already released its Connection" }
        return connection!!.sendPreparedStatement(query, parameters).use { statementResult ->
            if (statementResult.size == 0) {
                throw NoResultFound(query)
            }
            statementResult.first()
                .use { queryResult -> queryResult.extractFirst(rowParser) }
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
    fun <T : Any, R : RowParser<T>> fetchSingle(rowParser: R): T {
        checkNotNull(connection) { "Query already released its Connection" }
        return connection!!.sendPreparedStatement(query, parameters).use { statementResult ->
            if (statementResult.size == 0) {
                throw NoResultFound(query)
            }
            statementResult.first()
                .use { queryResult ->
                    if (queryResult.rowsAffected > 1) {
                        throw TooManyRows(query)
                    }
                    queryResult.extractFirst(rowParser) ?: throw EmptyQueryResult(query)
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
    fun <T : Any, R : RowParser<T>> fetchAll(rowParser: R): List<T> {
        checkNotNull(connection) { "Query already released its Connection" }
        return connection!!.sendPreparedStatement(query, parameters).use { statementResult ->
            if (statementResult.size == 0) {
                throw NoResultFound(query)
            }
            statementResult.first()
                .use { queryResult -> queryResult.extractAll(rowParser).toList() }
        }
    }

    /**
     * Execute the query and return the all rows as a [Sequence] where each row is parsed as the
     * type [T] by the supplied [rowParser]. Resulting [Sequence] is cold so the connection is
     * still in use until every row is collected or the [Sequence] is canceled.
     *
     * @throws IllegalStateException if the query has already been closed
     * @throws NoResultFound if the execution result yields no [QueryResult]
     * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
     * [RowParseError] are wrapped into a [RowParseError]
     */
    fun <T : Any, R : RowParser<T>> fetch(rowParser: R): Sequence<T> = sequence {
        checkNotNull(connection) { "Query already released its Connection" }
        connection!!.sendPreparedStatement(query, parameters).use { statementResult ->
            if (statementResult.size == 0) {
                throw NoResultFound(query)
            }
            statementResult.first()
                .use { queryResult ->
                for (row in queryResult.rows) {
                    try {
                        yield(rowParser.fromRow(row))
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
}
