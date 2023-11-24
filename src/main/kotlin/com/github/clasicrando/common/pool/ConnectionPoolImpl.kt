package com.github.clasicrando.common.pool

import com.github.clasicrando.common.Connection
import com.github.clasicrando.common.atomic.AtomicMutableSet
import com.github.clasicrando.common.result.QueryResult
import io.klogging.Klogging
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.coroutines.CoroutineContext

class ConnectionPoolImpl(
    private val poolOptions: PoolOptions,
    private val factory: ConnectionFactory,
) : ConnectionPool, Klogging {
    private val connections = Channel<PoolConnection>(capacity = poolOptions.maxConnections)
    private val connectionIds: MutableSet<String> = AtomicMutableSet()
    private val neededConnections = atomic(poolOptions.minConnections.coerceAtLeast(0))

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
            while (isActive) {
                val needed = neededConnections.getAndSet(0)
                for (i in 1..needed) {
                    addNewConnection()
                }
                delay(50)
            }
        }
    }

    private fun invalidateConnection(connection: PoolConnection) = launch {
        var connectionId = ""
        try {
            connectionId = connection.connectionId
            logger.trace("Invalidating connection id = {id}", connectionId)
            connection.pool = null
            connection.close()
        } catch (ex: Throwable) {
            logger.error(
                ex,
                "Error while closing invalid connection, '{connectionId}'",
                connectionId,
            )
        }
        connectionIds.remove(connectionId)
    }

    override suspend fun acquire(): Connection {
        while (true) {
            val possibleConnection = withTimeoutOrNull(500) {
                connections.receive()
            }
            if (possibleConnection == null && !exhausted()) {
                neededConnections.incrementAndGet()
            }
            val connection = possibleConnection ?: connections.receive()
            if (factory.validate(connection)) {
                return connection
            }
            invalidateConnection(connection)
        }
    }

    override suspend fun exhausted(): Boolean {
        return connectionIds.size == poolOptions.maxConnections
    }

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