package com.github.clasicrando.common.pool

import com.github.clasicrando.common.atomic.AtomicMutableMap
import com.github.clasicrando.common.connection.Connection
import com.github.clasicrando.common.exceptions.CouldNotInitializeConnection

abstract class AbstractPoolManager<O : Any, C : Connection> : PoolManager<O, C> {
    private val connectionPools: MutableMap<O, ConnectionPool<C>> = AtomicMutableMap()

    abstract fun createPool(options: O): ConnectionPool<C>

    override suspend fun createConnection(connectOptions: O): C {
        return connectionPools.getOrPut(connectOptions) {
            val pool = createPool(connectOptions)
            val isValid = pool.waitForValidation()
            if (!isValid) {
                throw CouldNotInitializeConnection(connectOptions.toString())
            }
            pool
        }.acquire()
    }

    override suspend fun closeAllPools() {
        for ((_, pool) in connectionPools) {
            pool.close()
        }
    }
}
