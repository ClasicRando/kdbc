package io.github.clasicrando.kdbc.postgresql.listen

import io.github.clasicrando.kdbc.core.AutoCloseableAsync
import io.github.clasicrando.kdbc.core.quoteIdentifier
import io.github.clasicrando.kdbc.postgresql.connection.PgConnection
import io.github.clasicrando.kdbc.postgresql.notification.PgNotification
import io.github.clasicrando.kdbc.postgresql.pool.PgConnectionPool

/** Take the next available connection from this pool and use it for a [PgListener] */
suspend fun PgConnectionPool.listener(): PgListener {
    return PgListener(acquire())
}

/**
 * Dedicated asynchronous listener class for receiving asynchronous [PgNotification]s sent from
 * other connections.
 */
class PgListener internal constructor(
    internal val connection: PgConnection,
) : AutoCloseableAsync {
    /**
     * Execute a `LISTEN` command for the specified [channelName]s. Allows the underlining
     * connection to receive notifications sent to this connection's current database. Notifications
     * can be received using the [receiveNotification] method.
     */
    suspend fun listen(vararg channelName: String) {
        val query =
            channelName.joinToString(
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
        val query = "UNLISTEN ${channelName.quoteIdentifier()};"
        connection.sendSimpleQuery(query)
    }

    /** Execute an `UNLISTEN *` command. Disables all notification channels for the underlining */
    suspend fun unlistenAll() {
        connection.sendSimpleQuery("UNLISTEN *;")
    }

    /**
     * Suspends until the next [PgNotification] is available from the connection.
     */
    suspend fun receiveNotification(): PgNotification {
        val result = connection.stream.notifications.tryReceive()
        check(!result.isClosed) { "Cannot receive from a close channel" }
        if (result.isSuccess) {
            return result.getOrNull()!!
        }
        return connection.stream.waitForNotificationOrError()
    }

    override suspend fun close() {
        unlistenAll()
        connection.close()
    }
}
