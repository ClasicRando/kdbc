package com.github.clasicrando.common.pool

import com.github.clasicrando.common.connection.Connection
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

object KdbcPoolsManager {
    private val lock = Mutex()
    private val poolManagers = mutableListOf<PoolManager<*, *>>()

    suspend fun <O : Any, C : Connection> addPoolManager(
        poolManager: PoolManager<O, C>
    ): Unit = lock.withLock {
        poolManagers.add(poolManager)
    }

    suspend fun closeAllPools(): Unit = lock.withLock {
        for (poolManager in poolManagers) {
            poolManager.closeAllPools()
        }
    }
}
