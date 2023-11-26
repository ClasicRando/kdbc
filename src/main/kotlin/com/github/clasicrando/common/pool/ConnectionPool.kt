package com.github.clasicrando.common.pool

import com.github.clasicrando.common.Connection
import com.github.clasicrando.common.Executor
import kotlinx.coroutines.CoroutineScope

interface ConnectionPool : Executor, CoroutineScope {
    val isExhausted: Boolean
    suspend fun acquire(): Connection
    suspend fun giveBack(connection: Connection): Boolean
    suspend fun <R> useConnection(block: suspend (Connection) -> R): R {
        var connection: Connection? = null
        var cause: Throwable? = null
        return try {
            connection = acquire()
            block(connection)
        } catch (ex: Throwable) {
            cause = ex
            throw ex
        } finally {
            try {
                connection?.close()
            } catch (ex: Throwable) {
                cause?.addSuppressed(ex)
                throw cause ?: ex
            }
        }
    }
}
