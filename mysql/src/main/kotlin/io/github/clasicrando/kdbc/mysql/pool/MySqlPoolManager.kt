package io.github.clasicrando.kdbc.mysql.pool

import io.github.clasicrando.kdbc.core.pool.BasePoolManager
import io.github.clasicrando.kdbc.core.pool.ConnectionPool
import io.github.clasicrando.kdbc.core.pool.PoolOptions
import io.github.clasicrando.kdbc.mysql.connection.MySqlConnection
import io.github.clasicrando.kdbc.mysql.connection.MySqlConnectionOptions

/**
 * MySQL specific implementation for a [BasePoolManager] that keeps track of [MySqlConnectionPool]
 * instances per unique [MySqlConnectionOptions].
 */
internal object MySqlPoolManager : BasePoolManager<MySqlConnectionOptions, MySqlConnection>() {
    override fun createPool(options: MySqlConnectionOptions): ConnectionPool<MySqlConnection> =
        MySqlConnectionPool(connectOptions = options, poolOptions = PoolOptions())
}
