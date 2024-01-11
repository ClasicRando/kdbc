package com.github.clasicrando.postgresql.pool

import com.github.clasicrando.common.pool.AbstractConnectionPool
import com.github.clasicrando.common.pool.PoolOptions
import com.github.clasicrando.postgresql.notification.PgListener
import com.github.clasicrando.postgresql.notification.PgListenerImpl
import com.github.clasicrando.postgresql.notification.PgNotificationConnection

class PgConnectionPoolImpl(
    poolOptions: PoolOptions,
    provider: PgConnectionProvider,
) : AbstractConnectionPool(poolOptions, provider), PgConnectionPool {
    override suspend fun createListener(): PgListener {
        return PgListenerImpl(acquire() as PgNotificationConnection)
    }
}
