package io.github.clasicrando.kdbc.core.connection

import io.github.clasicrando.kdbc.core.AutoCloseableAsync
import io.github.clasicrando.kdbc.core.UniqueResourceId
import io.github.clasicrando.kdbc.core.query.Query
import io.github.clasicrando.kdbc.core.result.StatementResult
import io.github.clasicrando.kdbc.core.use

private const val RESOURCE_TYPE = "Connection"

/**
 * A connection/session with a database. Each database vendor will provide these required properties
 * and methods to interact with any database implemented for this library.
 *
 * When you receive an instance of a [Connection], the underling connection has already been
 * established and the session has been prepared for immediate action.
 *
 * To keep the [Connection] resources from leaking, the recommended usage of [Connection] instances
 * is to utilize the [use] and [transaction] methods which always clean up [Connection] resources
 * before exiting. This does implicitly close the connection so if you intend to hold a [Connection]
 * for a long period of time (e.g. outside the scope of a single method) you should find a way to
 * always close the [Connection].
 */
public interface Connection : UniqueResourceId, AutoCloseableAsync {
    override val resourceType: String
        get() = RESOURCE_TYPE

    /**
     * Returns true if the underlining connection is still active and false if the connection has
     * been closed or a fatal error has occurred causing the connection to be aborted
     */
    public val isConnected: Boolean

    /** Returns true if the connection is currently within a transaction */
    public val inTransaction: Boolean

    /**
     * Request that the database start a new transaction. This will fail if the [Connection] is
     * already within a transaction.
     */
    public suspend fun begin()

    /**
     * Commit the current transaction. This will fail if the [Connection] was not within a
     * transaction
     */
    public suspend fun commit()

    /**
     * Rollback the current transaction. This will fail if the [Connection] was not within a
     * transaction
     */
    public suspend fun rollback()

    /**
     * Execute a single [Query] against this connection and returns the zero or more result sets as
     * a single [StatementResult].
     *
     * This sends the query to the database for execution and waits for all results to be sent to
     * the client before returning. Although this may require buffering more resources on the
     * client, it allows the connection state to be more consistent and not require the use of
     * database cursors to buffer results. The operation is also non-blocking while waiting for the
     * server response and will only resume processing results once the server replies so waiting
     * for all the data should not hold back your application given enough concurrency bandwidth.
     */
    public suspend fun executeQuery(query: Query): StatementResult

    /**
     * Execute a zero or more [Query]s against this connection and returns the zero or more result
     * sets as a single [StatementResult].
     *
     * The actual implementation of the batching will vary from driver to driver and will fall back
     * to simple sequential query execution if the database does not support query batching
     * natively. Also, by default query batches are executed in isolation so if the second query
     * fails the first query's action will be commited (if it modified the database). You can get
     * around this by manually starting a transaction before executing the batch or consulting the
     * specific driver to see if it permits a custom method that batches queries and handles the
     * entire operation in a single transaction.
     *
     * This sends the queries to the database for execution and waits for all results to be sent to
     * the client before returning. Although this may require buffering more resources on the
     * client, it allows the connection state to be more consistent and not require the use of
     * database cursors to buffer results. The operation is also non-blocking while waiting for the
     * server response and will only resume processing results once the server replies so waiting
     * for all the data should not hold back your application given enough concurrency bandwidth.
     */
    public suspend fun executeQueryBatch(batch: List<Query>): StatementResult

    /**
     * Execute a zero or more [Query]s against this connection and returns the zero or more result
     * sets as a single [StatementResult].
     *
     * The actual implementation of the batching will vary from driver to driver and will fall back
     * to simple sequential query execution if the database does not support query batching
     * natively. Also, by default query batches are executed in isolation so if the second query
     * fails the first query's action will be commited (if it modified the database). You can get
     * around this by manually starting a transaction before executing the batch or consulting the
     * specific driver to see if it permits a custom method that batches queries and handles the
     * entire operation in a single transaction.
     *
     * This sends the queries to the database for execution and waits for all results to be sent to
     * the client before returning. Although this may require buffering more resources on the
     * client, it allows the connection state to be more consistent and not require the use of
     * database cursors to buffer results. The operation is also non-blocking while waiting for the
     * server response and will only resume processing results once the server replies so waiting
     * for all the data should not hold back your application given enough concurrency bandwidth.
     */
    public suspend fun executeQueryBatch(vararg batch: Query): StatementResult
}

/**
 * Use a [Connection] within the scope of a transaction. This means an implicit [Connection.begin]
 * happens before [block] is called. If no exception is thrown then [Connection.commit] is called,
 * otherwise, [Connection.rollback] is called and the original exception is rethrown. This all
 * happens within a [AutoCloseableAsync.use] block so the resources are always cleaned up before
 * returning.
 */
public suspend inline fun <R, C : Connection> C.transaction(block: (C) -> R): R {
    return try {
        this.begin()
        val result = block(this)
        commit()
        result
    } catch (ex: Throwable) {
        rollback()
        throw ex
    }
}

/**
 * Use a [Connection] within the scope of a transaction. This means an implicit [Connection.begin]
 * happens before [block] is called. If no exception is thrown then [Connection.commit] is called,
 * returning the outcome of [block] as a [Result]. Otherwise, [Connection.rollback] is called and
 * the original exception is wrapped into a [Result] and returned. This all happens within a
 * [runCatching] block so all exceptions are caught and returned as a [Result].
 */
public suspend inline fun <R, C : Connection> C.transactionCatching(block: (C) -> R): Result<R> {
    return runCatching { transaction(block) }
}
