package com.github.clasicrando.postgresql.connection

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals

class TestListenNotifySpec {
    @Test
    fun `listen should issue a listen command and receive notification`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            it.listen(CHANNEL_NAME)
            it.sendQuery("select pg_notify('$CHANNEL_NAME', '$PAYLOAD')")
            val notification = withTimeout(200) {
                it.notifications.receive()
            }
            assertEquals(CHANNEL_NAME, notification.channelName)
            assertEquals(PAYLOAD, notification.payload)
        }
    }


    @Test
    fun `notify should issue a notify command and receive notification`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            it.sendQuery("LISTEN $CHANNEL_NAME")
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
