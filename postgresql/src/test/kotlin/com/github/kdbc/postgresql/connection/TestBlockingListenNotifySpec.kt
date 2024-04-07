package com.github.kdbc.postgresql.connection

import com.github.kdbc.core.connection.use
import com.github.kdbc.postgresql.PgConnectionHelper
import kotlin.test.Test
import kotlin.test.assertEquals

class TestBlockingListenNotifySpec {
    @Test
    fun `listen should issue a listen command and receive notification`() {
        PgConnectionHelper.defaultBlockingConnection().use {
            it.listen(CHANNEL_NAME)
            it.sendQuery("select pg_notify('$CHANNEL_NAME', '$PAYLOAD')")
            val notifications = it.getNotifications()
            assertEquals(
                1,
                notifications.size,
                "Notifications list must be exactly 1 item but got ${notifications.joinToString()}"
            )
            val notification = notifications.first()
            assertEquals(CHANNEL_NAME, notification.channelName)
            assertEquals(PAYLOAD, notification.payload)
        }
    }


    @Test
    fun `notify should issue a notify command and receive notification`() {
        PgConnectionHelper.defaultBlockingConnection().use {
            it.sendQuery("LISTEN $CHANNEL_NAME")
            it.notify(CHANNEL_NAME, PAYLOAD)
            val notifications = it.getNotifications()
            assertEquals(
                1,
                notifications.size,
                "Notifications list must be exactly 1 item but got ${notifications.joinToString()}"
            )
            val notification = notifications.first()
            assertEquals(CHANNEL_NAME, notification.channelName)
            assertEquals(PAYLOAD, notification.payload)
        }
    }

    companion object {
        private const val CHANNEL_NAME = "test"
        private const val PAYLOAD = "This is a test"
    }
}
