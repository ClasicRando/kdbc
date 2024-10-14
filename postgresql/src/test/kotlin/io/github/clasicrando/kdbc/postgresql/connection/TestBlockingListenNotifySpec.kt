package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.query.StringRowParser
import io.github.clasicrando.kdbc.core.query.executeClosing
import io.github.clasicrando.kdbc.core.query.fetchAll
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class TestBlockingListenNotifySpec {
    @Test
    fun `listen should issue a listen command and receive notification`() {
        PgConnectionHelper.defaultBlockingListener().use {
            it.listen(CHANNEL_NAME)
            PgConnectionHelper.defaultBlockingConnection().use { conn ->
                conn.createQuery("select pg_notify('$CHANNEL_NAME', '$PAYLOAD')")
                    .executeClosing()
            }

            val notification = CompletableFuture.supplyAsync { it.receiveNotification() }
                .get(5, TimeUnit.SECONDS)
            assertEquals(CHANNEL_NAME, notification.channelName)
            assertEquals(PAYLOAD, notification.payload)
        }
    }

    @Test
    fun `listen many should issue a listen command and receive notification`() {
        PgConnectionHelper.defaultBlockingListener().use {
            it.listen(CHANNEL_NAME, CHANNEL_NAME2)
            PgConnectionHelper.defaultBlockingConnection().use { conn ->
                conn.createQuery("select pg_notify('$CHANNEL_NAME', '$PAYLOAD')")
                    .executeClosing()
                conn.createQuery("select pg_notify('$CHANNEL_NAME2', '$PAYLOAD')")
                    .executeClosing()
            }

            val notification1 = CompletableFuture.supplyAsync { it.receiveNotification() }
                .get(5, TimeUnit.SECONDS)
            assertNotNull(notification1)
            assertEquals(CHANNEL_NAME, notification1.channelName)
            assertEquals(PAYLOAD, notification1.payload)

            val notification2 = CompletableFuture.supplyAsync { it.receiveNotification() }
                .get(5, TimeUnit.SECONDS)
            assertNotNull(notification2)
            assertEquals(CHANNEL_NAME2, notification2.channelName)
            assertEquals(PAYLOAD, notification2.payload)
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `unlisten should remove channels from connections listeners`(unlistenAll: Boolean) {
        PgConnectionHelper.defaultBlockingListener().use {
            it.listen(CHANNEL_NAME)
            val channelsBefore = it.connection
                .createQuery(LISTENER_QUERY)
                .fetchAll(StringRowParser)
            assertEquals(1, channelsBefore.size)
            assertEquals(CHANNEL_NAME, channelsBefore[0])

            if (unlistenAll) {
                it.unlistenAll()
            } else {
                it.unlisten(CHANNEL_NAME)
            }
            val channelsAfter = it.connection
                .createQuery(LISTENER_QUERY)
                .fetchAll(StringRowParser)
            assertEquals(0, channelsAfter.size)
        }
    }

    @Test
    fun `notify should issue a notification`() {
        PgConnectionHelper.defaultBlockingListener().use {
            it.listen(CHANNEL_NAME)
            PgConnectionHelper.defaultBlockingConnection().use { conn ->
                conn.notify(CHANNEL_NAME, PAYLOAD)
            }
            val notification = CompletableFuture.supplyAsync { it.receiveNotification() }
                .get(5, TimeUnit.SECONDS)
            assertEquals(CHANNEL_NAME, notification.channelName)
            assertEquals(PAYLOAD, notification.payload)
        }
    }

    companion object {
        private const val CHANNEL_NAME = "test"
        private const val CHANNEL_NAME2 = "test2"
        private const val PAYLOAD = "This is a test"
        private val LISTENER_QUERY = """
            SELECT *
            FROM pg_listening_channels()
        """.trimIndent()
    }
}
