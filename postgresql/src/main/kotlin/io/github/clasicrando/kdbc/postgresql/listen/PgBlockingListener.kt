package io.github.clasicrando.kdbc.postgresql.listen

import io.github.clasicrando.kdbc.core.quoteIdentifier
import io.github.clasicrando.kdbc.postgresql.connection.PgBlockingConnection
import io.github.clasicrando.kdbc.postgresql.notification.PgNotification
import io.github.clasicrando.kdbc.postgresql.pool.PgBlockingConnectionPool

/** Take the next available connection from this pool and use it for a [PgBlockingListener] */
fun PgBlockingConnectionPool.listener(): PgBlockingListener {
    return PgBlockingListener(acquire())
}

/**
 * Dedicated blocking listener class for receiving asynchronous [PgNotification]s sent from other
 * connections.
 */
class PgBlockingListener internal constructor(
    internal val connection: PgBlockingConnection,
) : AutoCloseable {
    /**
     * Execute a `LISTEN` command for the specified [channelName]s. Allows the underlining
     * connection to receive notifications sent to this connection's current database. Notifications
     * can be received using the [receiveNotification] method.
     */
    fun listen(vararg channelName: String) {
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
    fun unlisten(channelName: String) {
        val query = "UNLISTEN ${channelName.quoteIdentifier()};"
        connection.sendSimpleQuery(query)
    }

    /** Execute an `UNLISTEN *` command. Disables all notification channels for the underlining */
    fun unlistenAll() {
        connection.sendSimpleQuery("UNLISTEN *;")
    }

    /**
     * Blocks until the next [PgNotification] is available from the connection.
     */
    fun receiveNotification(): PgNotification {
        val notification = connection.stream
            .notificationsQueue
            .poll()
        if (notification != null) {
            return notification
        }
        return connection.stream.waitForNotificationOrError()
    }

    override fun close() {
        unlistenAll()
        connection.close()
    }
}
