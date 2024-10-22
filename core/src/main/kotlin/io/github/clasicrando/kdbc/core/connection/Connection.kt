package io.github.clasicrando.kdbc.core.connection

import io.github.clasicrando.kdbc.core.AutoCloseableAsync
import io.github.clasicrando.kdbc.core.UniqueResourceId
import io.github.clasicrando.kdbc.core.query.Query
import io.github.clasicrando.kdbc.core.result.StatementResult
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.core.useCatching

private const val RESOURCE_TYPE = "Connection"

/**
 * A connection/session with a database. Each database vendor will provide these required
 * properties and methods to interact with any database implemented for this library.
 *
 * When you receive an instance of a [Connection], the underling connection has already
 * been established and the session has been prepared for immediate action.
 *
 * To keep the [Connection] resources from leaking, the recommended usage of
 * [Connection] instances is to utilize the [use] and [transaction] methods which always
 * clean up [Connection] resources before exiting. This does implicitly close the
 * connection so if you intend to hold a [Connection] for a long period of time (e.g.
 * outside the scope of a single method) you should find a way to always close the
 * [Connection].
 */
interface Connection :
    UniqueResourceId,
    AutoCloseableAsync {
    override val resourceType: String get() = RESOURCE_TYPE

    /**
     * Returns true if the underlining connection is still active and false if the connection has
     * been closed or a fatal error has occurred causing the connection to be aborted
     */
    val isConnected: Boolean

    /** Returns true if the connection is currently within a transaction */
    val inTransaction: Boolean

    /**
     * Request that the database start a new transaction. This will fail if the
     * [Connection] is already within a transaction.
     */
    suspend fun begin()

    /**
     * Commit the current transaction. This will fail if the [Connection] was not within
     * a transaction
     */
    suspend fun commit()

    /**
     * Rollback the current transaction. This will fail if the [Connection] was not
     * within a transaction
     */
    suspend fun rollback()

    /**
     *
     */
    suspend fun executeQuery(query: Query): StatementResult

    /**
     *
     */
    suspend fun executeQueryBatch(batch: List<Query>): StatementResult

    /**
     *
     */
    suspend fun executeQueryBatch(vararg batch: Query): StatementResult
}

/**
 * Use a [Connection] within the scope of a transaction. This means an implicit
 * [Connection.begin] happens before [block] is called. If no exception is thrown then
 * [Connection.commit] is called, otherwise, [Connection.rollback] is called
 * and the original exception is rethrown. This all happens within a [AutoCloseableAsync.use]
 * block so the resources are always cleaned up before returning.
 */
suspend inline fun <R, C : Connection> C.transaction(block: (C) -> R): R =
    try {
        this.begin()
        val result = block(this)
        commit()
        result
    } catch (ex: Throwable) {
        rollback()
        throw ex
    }

/**
 * Use a [Connection] within the scope of a transaction. This means an implicit
 * [Connection.begin] happens before [block] is called. If no exception is thrown then
 * [Connection.commit] is called, returning the outcome of [block] as a [Result].
 * Otherwise, [Connection.rollback] is called and the original exception is wrapped into
 * a [Result] and returned. This all happens within a [AutoCloseableAsync.useCatching] block so
 * the resources are always cleaned up before returning and all other exceptions are caught and
 * returned as a [Result].
 */
suspend inline fun <R, C : Connection> C.transactionCatching(block: (C) -> R): Result<R> =
    runCatching {
        transaction(block)
    }
