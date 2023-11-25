package com.github.clasicrando.common

import kotlinx.uuid.UUID

interface Connection : Executor {
    val isConnected: Boolean
    val inTransaction: Boolean
    val connectionId: UUID
    suspend fun close()
    suspend fun begin()
    suspend fun commit()
    suspend fun rollback()
    suspend fun <R> use(block: suspend (Connection) -> R): R {
        var cause: Throwable? = null
        return try {
            block(this)
        } catch (ex: Throwable) {
            cause = ex
            throw ex
        } finally {
            try {
                close()
            } catch (ex: Throwable) {
                cause?.let { ex.addSuppressed(it) }
                throw ex
            }
        }
    }
    suspend fun <R> transaction(block: suspend (Connection) -> R): R {
        try {
            this.begin()
            val result = block(this)
            commit()
            return result
        } catch (ex: Throwable) {
            rollback()
            throw ex
        }
    }
    suspend fun releasePreparedStatement(query: String)
}
