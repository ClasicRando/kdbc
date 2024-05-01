package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.exceptions.EmptyQueryResult
import io.github.clasicrando.kdbc.core.exceptions.IncorrectScalarType
import io.github.clasicrando.kdbc.core.exceptions.NoResultFound
import io.github.clasicrando.kdbc.core.exceptions.RowParseError
import io.github.clasicrando.kdbc.core.exceptions.TooManyRows
import io.github.clasicrando.kdbc.core.result.QueryResult
import io.github.clasicrando.kdbc.core.result.StatementResult
import io.github.clasicrando.kdbc.core.use
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlin.reflect.KClass

/**
 * Execute this query by calling [Query.execute], always closing the [Query] before returning the
 * [StatementResult].
 */
suspend fun Query.executeClosing(): StatementResult = use { execute() }

/**
 * Execute the query and return the first row's first column as the type [T]. Returns null if
 * the return value is null or the query result has no rows.
 *
 * **Note**: This is a terminal operation for the [Query] since it is always closed before
 * returning
 *
 * @throws IllegalStateException if the query has already been closed
 * @throws NoResultFound if the execution result yields no [QueryResult]
 * @throws IncorrectScalarType if the scalar value is not an instance of the type [T], this
 * checked by [KClass.isInstance] on the first value
 */
suspend inline fun <reified T : Any> Query.fetchScalar(): T? = use {
    execute().use { statementResult ->
        if (statementResult.size == 0) {
            throw NoResultFound(sql)
        }
        statementResult.first()
            .use { queryResult -> queryResult.extractScalar() }
    }
}

/**
 * Execute the query and return the first row parsed as the type [T] by the supplied
 * [rowParser]. Returns null if the query results no rows.
 *
 * **Note**: This is a terminal operation for the [Query] since it is always closed before
 * returning
 *
 * @throws IllegalStateException if the query has already been closed
 * @throws NoResultFound if the execution result yields no [QueryResult]
 * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
 * [RowParseError] are wrapped into a [RowParseError]
 */
suspend fun <T : Any, R : RowParser<T>> Query.fetchFirst(rowParser: R): T? = use {
    execute().use { statementResult ->
        if (statementResult.size == 0) {
            throw NoResultFound(sql)
        }
        statementResult.first()
            .use { queryResult -> queryResult.extractFirst(rowParser) }
    }
}

/**
 * Execute the query and return the first row parsed as the type [T] by the supplied
 * [rowParser].
 *
 * **Note**: This is a terminal operation for the [Query] since it is always closed before
 * returning
 *
 * @throws IllegalStateException if the query has already been closed
 * @throws NoResultFound if the execution result yields no [QueryResult]
 * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
 * [RowParseError] are wrapped into a [RowParseError]
 * @throws EmptyQueryResult if the query returns no rows
 * @throws TooManyRows if the [QueryResult.rowsAffected] value > 1
 */
suspend fun <T : Any, R : RowParser<T>> Query.fetchSingle(rowParser: R): T = use {
    execute().use { statementResult ->
        if (statementResult.size == 0) {
            throw NoResultFound(sql)
        }
        statementResult.first()
            .use { queryResult ->
                if (queryResult.rowsAffected > 1) {
                    throw TooManyRows(sql)
                }
                queryResult.extractFirst(rowParser) ?: throw EmptyQueryResult(sql)
            }
    }
}

/**
 * Execute the query and return the all rows in a [List] where each row is parsed as the type
 * [T] by the supplied [rowParser]. Returns an empty [List] when no rows are returned.
 *
 * **Note**: This is a terminal operation for the [Query] since it is always closed before
 * returning
 *
 * @throws IllegalStateException if the query has already been closed
 * @throws NoResultFound if the execution result yields no [QueryResult]
 * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
 * [RowParseError] are wrapped into a [RowParseError]
 */
suspend fun <T : Any, R : RowParser<T>> Query.fetchAll(rowParser: R): List<T> = use {
    execute().use { statementResult ->
        if (statementResult.size == 0) {
            throw NoResultFound(sql)
        }
        statementResult.first()
            .use { queryResult -> queryResult.extractAll(rowParser).toList() }
    }
}

/**
 * Execute the query and return the all rows as a [Flow] where each row is parsed as the type
 * [T] by the supplied [rowParser]. Resulting [Flow] is cold so the connection is still in use
 * until every row is collected or the [Flow] is canceled.
 *
 * **Note**: This is a terminal operation for the [Query] since it is always closed before
 * returning
 *
 * @throws IllegalStateException if the query has already been closed
 * @throws NoResultFound if the execution result yields no [QueryResult]
 * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
 * [RowParseError] are wrapped into a [RowParseError]
 */
fun <T : Any, R : RowParser<T>> Query.fetch(rowParser: R): Flow<T> = flow {
    this@fetch.use {
        execute().use { statementResult ->
            if (statementResult.size == 0) {
                throw NoResultFound(sql)
            }
            statementResult.first()
                .use { queryResult ->
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
}


/**
 * Execute this query by calling [BlockingQuery.execute], always closing the [BlockingQuery] before
 * returning the [StatementResult].
 */
fun BlockingQuery.executeClosing(): StatementResult = use { execute() }

/**
 * Execute the query and return the first row's first column as the type [T]. Returns null if
 * the return value is null or the query result has no rows.
 *
 * **Note**: This is a terminal operation for the [BlockingQuery] since it is always closed before
 * returning
 *
 * @throws IllegalStateException if the query has already been closed
 * @throws NoResultFound if the execution result yields no [QueryResult]
 * @throws IncorrectScalarType if the scalar value is not an instance of the type [T], this
 * checked by [KClass.isInstance] on the first value
 */
inline fun <reified T : Any> BlockingQuery.fetchScalar(): T? = use {
    execute().use { statementResult ->
        if (statementResult.size == 0) {
            throw NoResultFound(sql)
        }
        statementResult.first()
            .use { queryResult -> queryResult.extractScalar() }
    }
}

/**
 * Execute the query and return the first row parsed as the type [T] by the supplied
 * [rowParser]. Returns null if the query results no rows.
 *
 * **Note**: This is a terminal operation for the [BlockingQuery] since it is always closed before
 * returning
 *
 * @throws IllegalStateException if the query has already been closed
 * @throws NoResultFound if the execution result yields no [QueryResult]
 * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
 * [RowParseError] are wrapped into a [RowParseError]
 */
fun <T : Any, R : RowParser<T>> BlockingQuery.fetchFirst(rowParser: R): T? = use {
    execute().use { statementResult ->
        if (statementResult.size == 0) {
            throw NoResultFound(sql)
        }
        statementResult.first()
            .use { queryResult -> queryResult.extractFirst(rowParser) }
    }
}

/**
 * Execute the query and return the first row parsed as the type [T] by the supplied
 * [rowParser].
 *
 * **Note**: This is a terminal operation for the [BlockingQuery] since it is always closed before
 * returning
 *
 * @throws IllegalStateException if the query has already been closed
 * @throws NoResultFound if the execution result yields no [QueryResult]
 * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
 * [RowParseError] are wrapped into a [RowParseError]
 * @throws EmptyQueryResult if the query returns no rows
 * @throws TooManyRows if the [QueryResult.rowsAffected] value > 1
 */
fun <T : Any, R : RowParser<T>> BlockingQuery.fetchSingle(rowParser: R): T = use {
    execute().use { statementResult ->
        if (statementResult.size == 0) {
            throw NoResultFound(sql)
        }
        statementResult.first()
            .use { queryResult ->
                if (queryResult.rowsAffected > 1) {
                    throw TooManyRows(sql)
                }
                queryResult.extractFirst(rowParser) ?: throw EmptyQueryResult(sql)
            }
    }
}

/**
 * Execute the query and return the all rows in a [List] where each row is parsed as the type
 * [T] by the supplied [rowParser]. Returns an empty [List] when no rows are returned.
 *
 * **Note**: This is a terminal operation for the [BlockingQuery] since it is always closed before
 * returning
 *
 * @throws IllegalStateException if the query has already been closed
 * @throws NoResultFound if the execution result yields no [QueryResult]
 * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
 * [RowParseError] are wrapped into a [RowParseError]
 */
fun <T : Any, R : RowParser<T>> BlockingQuery.fetchAll(rowParser: R): List<T> = use {
    execute().use { statementResult ->
        if (statementResult.size == 0) {
            throw NoResultFound(sql)
        }
        statementResult.first()
            .use { queryResult -> queryResult.extractAll(rowParser).toList() }
    }
}

/**
 * Execute the query and return the all rows as a [Sequence] where each row is parsed as the type
 * [T] by the supplied [rowParser]. Resulting [Sequence] is cold so the connection is still in use
 * until every row is collected or the [Sequence] is canceled.
 *
 * **Note**: This is a terminal operation for the [BlockingQuery] since it is always closed before
 * returning
 *
 * @throws IllegalStateException if the query has already been closed
 * @throws NoResultFound if the execution result yields no [QueryResult]
 * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
 * [RowParseError] are wrapped into a [RowParseError]
 */
fun <T : Any, R : RowParser<T>> BlockingQuery.fetch(rowParser: R): Sequence<T> = sequence {
    this@fetch.use {
        execute().use { statementResult ->
            if (statementResult.size == 0) {
                throw NoResultFound(sql)
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
}

