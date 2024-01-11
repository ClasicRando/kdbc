package com.github.clasicrando.postgresql.pool

import com.github.clasicrando.common.pool.ConnectionPool
import com.github.clasicrando.postgresql.notification.PgListener

interface PgConnectionPool : ConnectionPool {
    suspend fun createListener(): PgListener
}
