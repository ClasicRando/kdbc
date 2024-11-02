package io.github.clasicrando.kdbc.mysql.connection

import io.github.clasicrando.kdbc.core.DefaultUniqueResourceId
import io.github.clasicrando.kdbc.core.connection.Connection
import io.github.clasicrando.kdbc.core.query.Query
import io.github.clasicrando.kdbc.core.result.StatementResult
import io.github.clasicrando.kdbc.mysql.pool.MySqlConnectionPool
import io.github.clasicrando.kdbc.mysql.stream.MySqlStream
import io.github.clasicrando.kdbc.mysql.type.MySqlTypeCache
import kotlinx.atomicfu.AtomicBoolean
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

class MySqlConnection internal constructor(
    internal val connectionOptions: MySqlConnectionOptions,
    internal val stream: MySqlStream,
    internal val pool: MySqlConnectionPool,
    @PublishedApi internal val typeCache: MySqlTypeCache,
) : DefaultUniqueResourceId(),
    Connection {
    private val _inTransaction: AtomicBoolean = atomic(false)
    override val inTransaction: Boolean get() = _inTransaction.value

    /**
     * Suspending [Mutex] to allow only 1 coroutine to execute queries against this connection.
     * Each query operation is wrapped in a [Mutex.withLock] to ensure fair but exclusive access to
     * the connection.
     */
    private val mutex = Mutex()

    override val isConnected: Boolean get() = stream.isConnected

    override suspend fun begin() {
        TODO("Not yet implemented")
    }

    override suspend fun commit() {
        TODO("Not yet implemented")
    }

    override suspend fun rollback() {
        TODO("Not yet implemented")
    }

    override suspend fun executeQuery(query: Query): StatementResult {
        TODO("Not yet implemented")
    }

    override suspend fun executeQueryBatch(batch: List<Query>): StatementResult {
        TODO("Not yet implemented")
    }

    override suspend fun executeQueryBatch(vararg batch: Query): StatementResult {
        TODO("Not yet implemented")
    }

    override suspend fun close() {
        TODO("Not yet implemented")
    }

    suspend fun dispose() {
    }

    companion object {
        internal suspend fun connect(
            connectionOptions: MySqlConnectionOptions,
            stream: MySqlStream,
            pool: MySqlConnectionPool,
        ): MySqlConnection {
        }
    }
}
