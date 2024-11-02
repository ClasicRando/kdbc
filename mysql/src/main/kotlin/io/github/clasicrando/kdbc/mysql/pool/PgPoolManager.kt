package io.github.clasicrando.kdbc.mysql.pool

import io.github.clasicrando.kdbc.core.pool.BasePoolManager
import io.github.clasicrando.kdbc.core.pool.ConnectionPool
import io.github.clasicrando.kdbc.core.pool.PoolOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgConnectOptions
import io.github.clasicrando.kdbc.postgresql.connection.PgConnection

/**
 * Postgresql specific implementation for a [BasePoolManager] that keeps track of
 * [MySqlConnectionPool] instances per unique [PgConnectOptions].
 */
internal object PgPoolManager : BasePoolManager<PgConnectOptions, PgConnection>() {
    override fun createPool(options: PgConnectOptions): ConnectionPool<PgConnection> =
        MySqlConnectionPool(connectOptions = options, poolOptions = PoolOptions())
}
