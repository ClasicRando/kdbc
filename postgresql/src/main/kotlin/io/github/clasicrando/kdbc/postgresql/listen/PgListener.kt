package io.github.clasicrando.kdbc.postgresql.listen

import io.github.clasicrando.kdbc.core.AutoCloseableAsync
import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.quoteIdentifier
import io.github.clasicrando.kdbc.postgresql.connection.PgConnection
import io.github.clasicrando.kdbc.postgresql.notification.PgNotification
import io.github.clasicrando.kdbc.postgresql.pool.PgConnectionPool
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow

/** Take the next available connection from this pool and use it for a [PgListener] */
public suspend fun PgConnectionPool.listener(): PgListener {
    return PgListener(acquire())
}

/**
 * Dedicated asynchronous listener class for receiving asynchronous [PgNotification]s sent from
 * other connections.
 */
public class PgListener internal constructor(internal val connection: PgConnection) :
    AutoCloseableAsync {
    /**
     * Execute a `LISTEN` command for the specified [channelName]s. Allows the underlining
     * connection to receive notifications sent to this connection's current database. Notifications
     * can be received using the [receiveNotification] method.
     */
    public suspend fun listen(vararg channelName: String) {
        val listenQuery =
            channelName.joinToString(separator = "; LISTEN", prefix = "LISTEN ", postfix = ";") {
                it.quoteIdentifier()
            }
        query(listenQuery).execute(connection)
    }

    /**
     * Execute an `UNLISTEN` command for the specified [channelName]. Removes the channel name from
     * channels that the underlining connection will receive notifications from.
     */
    public suspend fun unlisten(channelName: String) {
        val unlistenQuery = "UNLISTEN ${channelName.quoteIdentifier()}"
        query(unlistenQuery).execute(connection)
    }

    /**
     * Execute an `UNLISTEN *` command. Disables all notification channels for the underlining
     * connection
     */
    public suspend fun unlistenAll() {
        query("UNLISTEN *").execute(connection)
    }

    /**
     * Receives the next [PgNotification] available from the connection. This function suspends to
     * wait for a notification unless the connection has a queued notification that it previously
     * received from the server.
     */
    public suspend fun receiveNotification(): PgNotification {
        val result = connection.stream.notifications.tryReceive()
        check(!result.isClosed) { "Cannot receive from a closed channel" }
        if (result.isSuccess) {
            return result.getOrNull()!!
        }
        return connection.stream.waitForNotificationOrError()
    }

    /**
     * Create a flow wrapper over [receiveNotification] that buffers notifications in a
     * [channelFlow] for consumption by a downstream collector. The flow is infinite and will only
     * be stopped when the connection is closed (by the client or server) or the coroutine scope is
     * cancelled.
     */
    public fun getNotifications(): Flow<PgNotification> {
        return channelFlow { send(this@PgListener.receiveNotification()) }
    }

    override suspend fun close() {
        unlistenAll()
        connection.close()
    }
}
