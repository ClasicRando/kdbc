package com.github.clasicrando.sqlserver.connection

import com.github.clasicrando.common.pool.ConnectionPool
import com.github.clasicrando.common.pool.PoolConnection
import com.github.clasicrando.common.result.QueryResult
import com.github.clasicrando.sqlserver.stream.SqlServerStream
import kotlinx.atomicfu.AtomicBoolean
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.CoroutineScope
import kotlinx.uuid.UUID
import kotlinx.uuid.generateUUID

class SqlServerConnectionImpl private constructor(
    private val connectionOptions: SqlServerConnectOptions,
    private val scope: CoroutineScope,
    private val stream: SqlServerStream,
    override val connectionId: UUID = UUID.generateUUID(),
    override var pool: ConnectionPool? = null
) : SqlServerConnection, PoolConnection {
    var flushed = false

    private val _inTransaction: AtomicBoolean = atomic(false)
    override val inTransaction: Boolean get() = _inTransaction.value

    override val isConnected: Boolean = stream.isActive



    override suspend fun close() {
        TODO("Not yet implemented")
    }

    override suspend fun begin() {
        TODO("Not yet implemented")
    }

    override suspend fun commit() {
        TODO("Not yet implemented")
    }

    override suspend fun rollback() {
        TODO("Not yet implemented")
    }

    override suspend fun releasePreparedStatement(query: String) {
        TODO("Not yet implemented")
    }

    override suspend fun sendQuery(query: String): QueryResult {
        TODO("Not yet implemented")
    }

    override suspend fun sendPreparedStatement(
        query: String,
        parameters: List<Any?>,
        release: Boolean,
    ): QueryResult {
        TODO("Not yet implemented")
    }

    companion object {
        suspend fun connect(
            configuration: SqlServerConnectOptions,
            scope: CoroutineScope,
            stream: SqlServerStream,
        ): SqlServerConnectionImpl {
            var connection: SqlServerConnectionImpl? = null
            try {
                connection = SqlServerConnectionImpl(
                    configuration,
                    scope,
                    stream,
                )
                if (!connection.isConnected) {
                    error("Could not initialize a connection")
                }
                return connection
            } catch (ex: Throwable) {
                connection?.close()
                stream.close()
                throw ex
            }
        }
    }
}
