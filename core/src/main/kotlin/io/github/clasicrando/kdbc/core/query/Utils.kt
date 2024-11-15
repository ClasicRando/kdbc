package io.github.clasicrando.kdbc.core.query

import io.github.clasicrando.kdbc.core.connection.Connection
import io.github.clasicrando.kdbc.core.exceptions.EmptyQueryResult
import io.github.clasicrando.kdbc.core.exceptions.NoResultFound
import io.github.clasicrando.kdbc.core.exceptions.RowParseError
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.Either
import io.github.clasicrando.kdbc.core.result.QueryResult
import kotlin.reflect.KType
import kotlin.reflect.typeOf
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.toList

/**
 * Execute the query and return the raw [QueryResult] from the query execution. This discards any
 * rows returned from the database.
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

/** TODO */
private suspend fun Query.fetchFirstRow(connection: Connection): DataRow? {
    var first: DataRow? = null
    connection.executeQuery(this).collect { value ->
        when (value) {
            is Either.Right -> {
                if (first == null) {
                    first = value.inner
                }
            }
            else -> {}
        }
    }
    return first
}

/**
 * Execute the query and return the first row's first column as the type [T]. Returns null if the
 * return value is null or the query result has no rows.
 *
 * @throws NoResultFound if the execution result yields no rows
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
 * @throws NoResultFound if the execution result yields no rows
 * @throws io.github.clasicrando.kdbc.core.column.ColumnExtractError if the first column cannot be
 *   extracted as the desired [kType]
 */
@PublishedApi
internal suspend fun Query.fetchScalar(connection: Connection, kType: KType): Any? {
    return fetchFirstRow(connection)?.get(0, kType)
}

/**
 * Execute the query and return the first row parsed as the type [T] by the supplied [rowParser].
 * Returns null if the query results no rows.
 *
 * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
 *   [RowParseError] are wrapped into a [RowParseError]
 */
public suspend fun <T : Any, R : RowParser<T>> Query.fetchFirst(
    connection: Connection,
    rowParser: R,
): T? {
    val first: DataRow = fetchFirstRow(connection) ?: return null
    return try {
        rowParser.fromRow(first)
    } catch (ex: RowParseError) {
        throw ex
    } catch (ex: Exception) {
        throw RowParseError(rowParser, ex)
    }
}

/**
 * Execute the query and return the first row parsed as the type [T] by the supplied [rowParser].
 *
 * @throws EmptyQueryResult if the execution result yields no rows
 * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
 *   [RowParseError] are wrapped into a [RowParseError]
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
 * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
 *   [RowParseError] are wrapped into a [RowParseError]
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
 * @throws RowParseError if the [rowParser] throws any [Throwable], thrown errors other than
 *   [RowParseError] are wrapped into a [RowParseError]
 */
public suspend fun <T : Any, R : RowParser<T>> Query.fetch(
    connection: Connection,
    rowParser: R,
): Flow<T> {
    return connection.executeQuery(this).mapNotNull {
        when (it) {
            is Either.Left -> null
            is Either.Right ->
                try {
                    rowParser.fromRow(it.inner)
                } catch (ex: RowParseError) {
                    throw ex
                } catch (ex: Exception) {
                    throw RowParseError(rowParser, ex)
                }
        }
    }
}
