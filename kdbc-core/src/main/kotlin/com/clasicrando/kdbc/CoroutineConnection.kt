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
import kotlin.coroutines.coroutineContext

private val logger = KotlinLogging.logger {}

val CoroutineContext.connection: Connection
    get() = get(CoroutineConnection)?.connection ?: error("No connection in context")

@OptIn(ExperimentalContracts::class)
suspend inline fun <T> withConnection(crossinline block: suspend CoroutineScope.() -> T): T {
    contract {
        callsInPlace(block, InvocationKind.EXACTLY_ONCE)
    }
    return if (coroutineContext.hasOpenConnection()) {
        withContext(coroutineContext) {
            block()
        }
    } else {
        val connection = coroutineContext.dataSource.connection
        try {
            withContext(CoroutineConnection(connection)) {
                block()
            }
        } finally {
            connection.closeCatching()
        }
    }
}

@PublishedApi
internal fun Connection.closeCatching() {
    try {
        close()
    } catch (ex: SQLException) {
        logger.warn(ex) { "Exception thrown while trying to close a Connection" }
    }
}

internal fun Connection.isClosedCatching(): Boolean {
    return try {
        isClosed
    } catch (ex: SQLException) {
        logger.warn(ex) { "Connection isClosed check failed. Connection is assumed to be closed" }
        true
    }
}

@PublishedApi
internal fun CoroutineContext.hasOpenConnection(): Boolean {
    val connection = get(CoroutineConnection)?.connection
    return connection != null && !connection.isClosedCatching()
}

class CoroutineConnection(
    val connection: Connection
) : AbstractCoroutineContextElement(CoroutineConnection) {

    companion object Key : CoroutineContext.Key<CoroutineConnection>

    override fun toString() = "CoroutineConnection($connection)"
}
