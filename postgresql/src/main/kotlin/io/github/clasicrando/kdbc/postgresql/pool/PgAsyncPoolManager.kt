package io.github.clasicrando.kdbc.postgresql.pool

import io.github.clasicrando.kdbc.core.pool.AsyncConnectionPool
import io.github.clasicrando.kdbc.core.pool.BaseAsyncPoolManager
import io.github.clasicrando.kdbc.core.pool.PoolOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgAsyncConnection
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions

/**
 * Postgresql specific implementation for a [BaseAsyncPoolManager] that keeps track of
 * [PgAsyncConnectionPool] instances per unique [PgConnectOptions].
 */
internal object PgAsyncPoolManager : BaseAsyncPoolManager<PgConnectOptions, PgAsyncConnection>() {
    override fun createPool(options: PgConnectOptions): AsyncConnectionPool<PgAsyncConnection> {
        return PgAsyncConnectionPool(connectOptions = options, poolOptions = PoolOptions())
    }
}