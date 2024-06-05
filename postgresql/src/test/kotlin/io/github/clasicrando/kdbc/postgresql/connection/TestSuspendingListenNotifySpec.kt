package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.query.executeClosing
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals

class TestSuspendingListenNotifySpec {
    @Test
    fun `listen should issue a listen command and receive notification`(): Unit = runBlocking {
        PgConnectionHelper.defaultSuspendingConnection().use {
            it.listen(CHANNEL_NAME)
            it.createQuery("select pg_notify('$CHANNEL_NAME', '$PAYLOAD')")
                .executeClosing()
            val notification = withTimeout(200) {
                it.notifications.receive()
            }
            assertEquals(CHANNEL_NAME, notification.channelName)
            assertEquals(PAYLOAD, notification.payload)
        }
    }


    @Test
    fun `notify should issue a notify command and receive notification`(): Unit = runBlocking {
        PgConnectionHelper.defaultSuspendingConnection().use {
            it.createQuery("LISTEN $CHANNEL_NAME")
                .executeClosing()
            it.notify(CHANNEL_NAME, PAYLOAD)
            val notification = withTimeout(200) {
                it.notifications.receive()
            }
            assertEquals(CHANNEL_NAME, notification.channelName)
            assertEquals(PAYLOAD, notification.payload)
        }
    }

    companion object {
        private const val CHANNEL_NAME = "test"
        private const val PAYLOAD = "This is a test"
    }
}
