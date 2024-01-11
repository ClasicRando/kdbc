package com.github.clasicrando.postgresql.notification

import com.github.clasicrando.common.atomic.AtomicMutableSet
import kotlinx.coroutines.selects.SelectClause1

internal class PgListenerImpl(private val connection: PgNotificationConnection) : PgListener {
    private val channelNames: MutableSet<String> = AtomicMutableSet()

    private fun quoteChannelName(channelName: String): String {
        return channelName.replace("\"", "\"\"")
    }

    override suspend fun listen(channelName: String) {
        connection.sendQuery("LISTEN \"${quoteChannelName(channelName)}\"")
        channelNames.add(channelName)
    }

    override suspend fun unlisten(channelName: String) {
        connection.sendQuery("UNLISTEN \"${quoteChannelName(channelName)}\"")
        channelNames.remove(channelName)
    }

    override suspend fun unlistenAll() {
        connection.sendQuery("UNLISTEN *")
        channelNames.clear()
    }

    override suspend fun receiveNotification(): PgNotification {
        return connection.notificationsChannel.receive()
    }

    override val onReceive: SelectClause1<PgNotification>
        get() = connection.notificationsChannel.onReceive

    override suspend fun close() {
        connection.close()
    }

    override fun toString(): String {
        return "PgListenerImpl(connectionId=${connection.connectionId},channels=$channelNames)"
    }
}
