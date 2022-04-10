package com.clasicrando.kdbc

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import java.sql.Connection
import java.sql.SQLException
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.coroutineContext

private val logger = KotlinLogging.logger {}

/**
 * Getter to obtain a connection from the current coroutineContext
 *
 * @throws IllegalStateException the current context has no connection element
 */
val CoroutineContext.connection: Connection
    get() = get(CoroutineConnection)?.connection ?: error("No connection in context")

/**
 * Performs the specific action within [block], adding a [Connection] to the coroutine context if needed. The provided
 * [context] must contain a [CoroutineDataSource] to have a connection available to the action. If the current context
 * and the provided context do not contain an open connection, the [DataSource][javax.sql.DataSource] is used to obtain
 * a new connection (which will be closed no matter what) that is added to the new [CoroutineContext].
 *
 * @throws IllegalStateException the dataSource/connection [CoroutineContext] elements cannot be found
 */
@PublishedApi
@OptIn(ExperimentalContracts::class)
internal suspend inline fun <T> withConnection(
    context: CoroutineContext,
    crossinline block: suspend CoroutineScope.() -> T,
): T {
    contract {
        callsInPlace(block, InvocationKind.EXACTLY_ONCE)
    }
    val mergedContext = coroutineContext + context
    val extraContext = if (mergedContext.hasOpenConnection()) {
        EmptyCoroutineContext
    } else {
        CoroutineConnection(mergedContext.dataSource.connection)
    }
    return withContext(mergedContext + extraContext) {
        try {
            block()
        } finally {
            if (extraContext === EmptyCoroutineContext) {
                coroutineContext.connection.closeCatching()
            }
        }
    }
}

/**
 * Suppresses [SQLException]s when trying to close a [Connection]. Logs the event but doesn't rethrow the exception
 */
@PublishedApi
internal fun Connection.closeCatching() {
    try {
        close()
    } catch (ex: SQLException) {
        logger.warn(ex) { "Exception thrown while trying to close a Connection" }
    }
}

/**
 * Checks to see if the [Connection] is closed and returns the [isClosed][Connection.isClosed] property. Suppresses
 * [SQLException]s and treats a caught exception as a closed connection.
 */
internal fun Connection.isClosedCatching(): Boolean {
    return try {
        isClosed
    } catch (ex: SQLException) {
        logger.warn(ex) { "Connection isClosed check failed. Connection is assumed to be closed" }
        true
    }
}

/**
 * Checks the current [CoroutineContext] to see if it contains an open [Connection] as a context element. If the current
 * context does not contain a connection or the connection in the context is closed, false is returned. Otherwise, true
 * is returned
 */
@PublishedApi
internal fun CoroutineContext.hasOpenConnection(): Boolean {
    val connection = get(CoroutineConnection)?.connection
    return connection != null && !connection.isClosedCatching()
}

/**
 * [CoroutineContext] element holding a [Connection]. Used to pass along the same connection to a coroutine even when
 * switching contexts, resuming execution or switching [Dispatchers][kotlinx.coroutines.Dispatchers]
 */
class CoroutineConnection(
    val connection: Connection
) : AbstractCoroutineContextElement(CoroutineConnection) {

    companion object Key : CoroutineContext.Key<CoroutineConnection>

    override fun toString() = "CoroutineConnection($connection)"
}
