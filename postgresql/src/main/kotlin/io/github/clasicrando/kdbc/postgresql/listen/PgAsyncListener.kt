package io.github.clasicrando.kdbc.postgresql.listen

import io.github.clasicrando.kdbc.core.AutoCloseableAsync
import io.github.clasicrando.kdbc.core.deferredDelay
import io.github.clasicrando.kdbc.core.quoteIdentifier
import io.github.clasicrando.kdbc.postgresql.connection.PgAsyncConnection
import io.github.clasicrando.kdbc.postgresql.notification.PgNotification
import io.github.clasicrando.kdbc.postgresql.pool.PgAsyncConnectionPool
import kotlinx.coroutines.selects.select
import kotlin.time.DurationUnit
import kotlin.time.toDuration


/** Take the next available connection from this pool and use it for a [PgAsyncListener] */
suspend fun PgAsyncConnectionPool.listener(): PgAsyncListener {
    return PgAsyncListener(acquire())
}

private val HALF_SECOND_DELAY = 500.toDuration(DurationUnit.MILLISECONDS)

/**
 * Dedicated asynchronous listener class for receiving asynchronous [PgNotification]s sent from
 * other connections.
 */
class PgAsyncListener internal constructor(
    internal val connection: PgAsyncConnection,
) : AutoCloseableAsync {
    /**
     * Execute a `LISTEN` command for the specified [channelName]s. Allows the underlining
     * connection to receive notifications sent to this connection's current database. Notifications
     * can be received using the [receiveNotification] and [tryReceiveNotification] methods.
     */
    suspend fun listen(vararg channelName: String) {
        val query = channelName.joinToString(
            separator = "; LISTEN",
            prefix = "LISTEN ",
            postfix = ";",
        ) { it.quoteIdentifier() }
        connection.sendSimpleQuery(query)
    }

    /**
     * Execute an `UNLISTEN` command for the specified [channelName]. Removes the channel name from
     * channels that the underlining connection will receive notifications from.
     */
    suspend fun unlisten(channelName: String) {
        val query = "UNLISTEN ${channelName.quoteIdentifier()}"
        connection.sendSimpleQuery(query)
    }

    /** Execute an `UNLISTEN *` command. Disables all notification channels for the underlining */
    suspend fun unlistenAll() {
        connection.sendSimpleQuery("UNLISTEN *")
    }

    /**
     * Suspends until the next [PgNotification] is available from the connection.
     */
    suspend fun receiveNotification(): PgNotification {
        while (true) {
            connection.stream.flushMessages()
            val notification: PgNotification? = select {
                connection.stream.notifications.onReceive { it }
                connection.pool.deferredDelay(HALF_SECOND_DELAY).onAwait { null }
            }
            if (notification != null) {
                return notification
            }
        }
    }

    /**
     * Attempts to receive the next [PgNotification] from the connection, returning null if there
     * are no notifications available.
     *
     * @throws IllegalStateException if the channel storing the notifications is closed
     */
    suspend fun tryReceiveNotification(): PgNotification? {
        connection.stream.flushMessages()
        val result = connection.stream.notifications.tryReceive()
        check(!result.isClosed) { "Cannot receive from a close channel" }
        return result.getOrNull()
    }

    override suspend fun close() {
        unlistenAll()
        connection.close()
    }
}
