package com.github.clasicrando.common.pool

import com.github.clasicrando.common.connection.Connection
import com.github.clasicrando.common.atomic.AtomicMutableSet
import com.github.clasicrando.common.result.QueryResult
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.uuid.UUID
import kotlin.coroutines.CoroutineContext

private val logger = KotlinLogging.logger {}

class ConnectionPoolImpl(
    private val poolOptions: PoolOptions,
    private val factory: ConnectionFactory,
) : ConnectionPool {
    private val connections = Channel<PoolConnection>(capacity = poolOptions.maxConnections)
    private val connectionIds: MutableSet<UUID> = AtomicMutableSet()
    private val connectionsNeeded = Channel<Unit>(capacity = poolOptions.maxConnections)

    override val coroutineContext: CoroutineContext
        get() = SupervisorJob(
            parent = poolOptions.parentScope?.coroutineContext?.job,
        ) + poolOptions.coroutineDispatcher

    private suspend fun addNewConnection() {
        val connection = factory.create(this@ConnectionPoolImpl) as PoolConnection
        connection.pool = this@ConnectionPoolImpl
        connectionIds.add(connection.connectionId)
        connections.send(connection)
    }

    init {
        launch {
            var cause: Throwable? = null
            try {
                while (isActive) {
                    connectionsNeeded.receive()
                    if (isExhausted) {
                        continue
                    }
                    addNewConnection()
                }
            } catch (ex: CancellationException) {
                cause = ex
                throw ex
            } catch (ex: Throwable) {
                this.cancel(message = "Error creating new connection", cause = ex)
                cause = ex
                throw ex
            } finally {
                connections.close(cause = cause)
                connectionsNeeded.close(cause = cause)
                logger.atError {
                    message = "Exiting pool observer"
                }
            }
        }
    }

    private fun invalidateConnection(connection: PoolConnection) = launch {
        var connectionId: UUID? = null
        try {
            connectionId = connection.connectionId
            logger.atTrace {
                message = "Invalidating connection id = {id}"
                payload = mapOf("id" to connectionId)
            }
            connection.pool = null
            connection.close()
            connectionIds.remove(connectionId)
        } catch (ex: Throwable) {
            logger.atError {
                cause = ex
                message = "Error while closing invalid connection, '{connectionId}'"
                payload = connectionId?.let { mapOf("connectionId" to it) }
            }
        }
    }

    override suspend fun acquire(): Connection {
        while (true) {
            val possibleConnection = withTimeoutOrNull(50) {
                connections.receive()
            }
            if (possibleConnection == null) {
                connectionsNeeded.send(Unit)
            }
            val connection = possibleConnection ?: connections.receive()
            if (factory.validate(connection)) {
                return connection
            }
            invalidateConnection(connection)
        }
    }

    override val isExhausted: Boolean get() = connectionIds.size == poolOptions.maxConnections

    override suspend fun sendQuery(query: String): QueryResult {
        var connection: Connection? = null
        return try {
            connection = acquire()
            connection.sendQuery(query)
        } catch (ex: Throwable) {
            logger.atError {
                cause = ex
                message = "Error sending raw query, {connectionId}"
                payload = connection?.connectionId?.let { mapOf("connectionId" to it) }
            }
            throw ex
        } finally {
            connection?.close()
        }
    }

    override suspend fun sendPreparedStatement(
        query: String,
        parameters: List<Any?>,
        release: Boolean,
    ): QueryResult {
        var connection: Connection? = null
        return try {
            connection = acquire()
            connection.sendPreparedStatement(query, parameters)
        } catch (ex: Throwable) {
            logger.atError {
                cause = ex
                message = "Error sending prepared statement, {connectionId}"
                payload = connection?.connectionId?.let { mapOf("connectionId" to it) }
            }
            throw ex
        } finally {
            connection?.close()
        }
    }

    override suspend fun giveBack(connection: Connection): Boolean {
        val poolConnection = connection as? PoolConnection ?: return false
        if (!connectionIds.contains(poolConnection.connectionId)) {
            return false
        }
        if (!factory.validate(poolConnection)) {
            invalidateConnection(poolConnection)
            return true
        }
        connections.send(poolConnection)
        return true
    }
}