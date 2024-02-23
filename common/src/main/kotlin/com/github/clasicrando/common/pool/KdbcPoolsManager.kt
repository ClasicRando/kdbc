package com.github.clasicrando.common.pool

import com.github.clasicrando.common.atomic.AtomicMutableMap
import com.github.clasicrando.common.connection.Connection
import kotlin.reflect.KClass

typealias PoolManagerClass = KClass<out PoolManager<*, *>>

object KdbcPoolsManager {
    private val poolManagers: MutableMap<PoolManagerClass, PoolManager<*, *>> = AtomicMutableMap()

    fun <O : Any, C : Connection> addPoolManager(
        poolManager: PoolManager<O, C>
    ) {
        poolManagers[poolManager::class] = poolManager
    }

    suspend fun closeAllPools() {
        for ((_, poolManager) in poolManagers) {
            poolManager.closeAllPools()
        }
    }
}
