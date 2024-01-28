package com.github.clasicrando.common.pool

import com.github.clasicrando.common.connection.Connection

interface PoolManager<O : Any, C : Connection> {
    suspend fun createConnection(connectOptions: O): C
    suspend fun closeAllPools()
}
