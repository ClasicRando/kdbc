package com.github.clasicrando.common.pool

import com.github.clasicrando.common.Connection
import com.github.clasicrando.common.Executor
import kotlinx.coroutines.CoroutineScope

interface ConnectionPool : Executor, CoroutineScope {
    suspend fun acquire(): Connection
    suspend fun exhausted(): Boolean
    suspend fun giveBack(connection: Connection): Boolean
    suspend fun <R> useConnection(block: suspend (Connection) -> R): R {
        return try {
            val connection = acquire()
            block(connection)
        } finally {

        }
    }
}
