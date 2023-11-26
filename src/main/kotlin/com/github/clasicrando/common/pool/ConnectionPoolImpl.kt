package com.github.clasicrando.common.pool

import com.github.clasicrando.common.Connection
import com.github.clasicrando.common.atomic.AtomicMutableSet
import com.github.clasicrando.common.result.QueryResult
import io.klogging.Klogging
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

class ConnectionPoolImpl(
    private val poolOptions: PoolOptions,
    private val factory: ConnectionFactory,
) : ConnectionPool, Klogging {
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
                logger.error("Exiting pool observer")
            }
        }
    }

    private fun invalidateConnection(connection: PoolConnection) = launch {
        var connectionId: UUID? = null
        try {
            connectionId = connection.connectionId
            logger.trace("Invalidating connection id = {id}", connectionId)
            connection.pool = null
            connection.close()
            connectionIds.remove(connectionId)
        } catch (ex: Throwable) {
            logger.error(
                ex,
                "Error while closing invalid connection, '{connectionId}'",
                connectionId,
            )
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
            logger.error(
                ex,
                "Error sending raw query, {connectionId}",
                connection?.connectionId,
            )
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
            logger.error(
                ex,
                "Error sending raw query, {connectionId}",
                connection?.connectionId,
            )
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