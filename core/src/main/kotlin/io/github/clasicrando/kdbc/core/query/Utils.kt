package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.connection.Connection
import io.github.clasicrando.kdbc.core.exceptions.EmptyQueryResult
import io.github.clasicrando.kdbc.core.exceptions.RowParseError
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.Either
import io.github.clasicrando.kdbc.core.result.QueryResult
import kotlin.reflect.KType
import kotlin.reflect.typeOf
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.toList

/**
 * Execute the query and return a [QueryResult] from the query execution. This discards any rows
 * returned from the database and merges all [QueryResult] instances if more than 1 are returned.
 *
 * @param connection [Connection] to execute the query against
 */
public suspend fun Query.execute(connection: Connection): QueryResult {
    return connection
        .executeQuery(this)
        .mapNotNull {
            when (it) {
                is Either.Left -> it.inner
                else -> null
            }
        }
        .fold(QueryResult(0, ""), QueryResult::merge)
}

/**
 * Execute this query against the supplied [connection], returning the first [DataRow] or null if no
 * data rows are returned. All subsequent rows are discarded.
 */
private suspend fun Query.fetchFirstRow(connection: Connection): DataRow? {
    var first: DataRow? = null
    connection.executeQuery(this).collect { value ->
        when (value) {
            is Either.Right if first == null -> first = value.inner
            else -> {}
        }
    }
    return first
}

/**
 * Execute the query and return the first row's first column as the type [T]. Returns null if the
 * return value is null or the query result has no rows.
 *
 * @throws EmptyQueryResult if the execution result yields no rows
 * @throws io.github.clasicrando.kdbc.core.column.ColumnExtractError if the first column cannot be
 *   extracted as the desired type [T]
 */
public suspend inline fun <reified T : Any> Query.fetchScalar(connection: Connection): T? {
    return fetchScalar(connection, typeOf<T>()) as T?
}

/**
 * Execute the query and return the first row's first column as [kType]. Returns null if the return
 * value is null or the query result has no rows.
 *
 * @throws EmptyQueryResult if the execution result yields no rows
 * @throws io.github.clasicrando.kdbc.core.column.ColumnExtractError if the first column cannot be
 *   extracted as the desired [kType]
 */
@PublishedApi
internal suspend fun Query.fetchScalar(connection: Connection, kType: KType): Any? {
    val row = fetchFirstRow(connection) ?: throw EmptyQueryResult("")
    return row.get(0, kType)
}

/**
 * Execute the query and return the first row parsed as the type [T] by the supplied [rowParser].
 * Returns null if the query results no rows.
 *
 * @throws RowParseError if the row parsing fails for any reason, see suppressed and cause
 *   exceptions for more details
 */
public suspend fun <T : Any, R : RowParser<T>> Query.fetchFirst(
    connection: Connection,
    rowParser: R,
): T? {
    val first: DataRow = fetchFirstRow(connection) ?: return null
    return first.parse(rowParser)
}

/**
 * Execute the query and return the first row parsed as the type [T] by the supplied [rowParser].
 *
 * @throws EmptyQueryResult if the execution result yields no rows
 * @throws RowParseError if the row parsing fails for any reason, see suppressed and cause
 *   exceptions for more details
 */
public suspend fun <T : Any, R : RowParser<T>> Query.fetchOne(
    connection: Connection,
    rowParser: R,
): T {
    return fetchFirst(connection, rowParser) ?: throw EmptyQueryResult(sql)
}

/**
 * Execute the query and return the all rows in a [List] where each row is parsed as the type [T] by
 * the supplied [rowParser]. Returns an empty [List] when no rows are returned.
 *
 * @throws RowParseError if the row parsing fails for any reason, see suppressed and cause
 *   exceptions for more details
 */
public suspend fun <T : Any, R : RowParser<T>> Query.fetchAll(
    connection: Connection,
    rowParser: R,
): List<T> {
    return fetch(connection, rowParser).toList()
}

/**
 * Execute the query and return the all rows as a [Flow] where each row is parsed as the type [T] by
 * the supplied [rowParser]. Resulting [Flow] is cold so the connection is still in use until every
 * row is collected or the [Flow] is canceled.
 *
 * @throws RowParseError if the row parsing fails for any reason, see suppressed and cause
 *   exceptions for more details
 */
public suspend fun <T : Any, R : RowParser<T>> Query.fetch(
    connection: Connection,
    rowParser: R,
): Flow<T> {
    return connection
        .executeQuery(this)
        .mapNotNull {
            when (it) {
                is Either.Left -> null
                is Either.Right -> it.inner
            }
        }
        .mapToRows(rowParser)
}

/**
 * Execute a batch of queries and collect all [QueryResult]s returned. If any [DataRow]s were
 * received from the database, they will be discarded. By default, this operation does not attempt
 * to wrap all the queries in a single transaction. Generally databases treat each query as a
 * separate transaction unless a transaction was explicitly started against the current session. To
 * explicitly tell the database driver to treat the batch as an atomic operation, set
 * [withinTransaction] to true.
 */
public suspend fun List<Query>.executeMany(
    connection: Connection,
    withinTransaction: Boolean = false,
): Flow<QueryResult> {
    return connection.executeQueryBatch(this@executeMany, withinTransaction).mapNotNull {
        when (it) {
            is Either.Left -> it.inner
            is Either.Right -> null
        }
    }
}

/**
 * Execute a batch of queries and collect each batch of [DataRow]s returned. By default, this
 * operation does not attempt to wrap all the queries in a single transaction. Generally databases
 * treat each query as a separate transaction unless a transaction was explicitly started against
 * the current session. To explicitly tell the database driver to treat the batch as an atomic
 * operation, set [withinTransaction] to true.
 */
public fun List<Query>.fetchMany(
    connection: Connection,
    withinTransaction: Boolean = false,
): Flow<List<DataRow>> {
    return flow {
        var buffer = mutableListOf<DataRow>()
        connection.executeQueryBatch(this@fetchMany, withinTransaction).collect {
            when (it) {
                is Either.Left -> {
                    emit(buffer)
                    buffer = mutableListOf()
                }
                is Either.Right -> buffer.add(it.inner)
            }
        }
    }
}

/**
 * Map this list of [DataRow]s using the [rowParser] supplied. This is equivalent to
 *
 * ```
 * this.map { it.parse(rowParser) }
 * ```
 *
 * @throws RowParseError if the row parsing fails for any reason, see suppressed and cause
 *   exceptions for more details
 */
public fun <T : Any, R : RowParser<T>> List<DataRow>.mapToRows(rowParser: R): List<T> {
    if (this.isEmpty()) {
        return emptyList()
    }
    return this.map { it.parse(rowParser) }
}

/**
 * Map this flow of [DataRow]s using the [rowParser] supplied. This is equivalent to
 *
 * ```
 * this.map { it.parse(rowParser) }
 * ```
 *
 * @throws RowParseError if the row parsing fails for any reason, see suppressed and cause
 *   exceptions for more details
 */
public fun <T : Any, R : RowParser<T>> Flow<DataRow>.mapToRows(rowParser: R): Flow<T> {
    return this.map { it.parse(rowParser) }
}

/**
 * Call [RowParser.fromRow] on this [DataRow], catching exceptions and wrapping them as
 * [RowParseError] if not already of that specific type.
 *
 * @throws RowParseError if the row parsing fails for any reason, see suppressed and cause
 *   exceptions for more details
 */
private fun <T : Any, R : RowParser<T>> DataRow.parse(rowParser: R): T {
    return try {
        rowParser.fromRow(this)
    } catch (ex: RowParseError) {
        throw ex
    } catch (ex: Exception) {
        throw RowParseError(rowParser, ex)
    }
}
