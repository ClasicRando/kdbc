package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.query.StringRowParser
import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.fetchAll
import io.github.clasicrando.kdbc.core.query.preparedQuery
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class TestListenNotifySpec {
    @Test
    fun `listen should issue a listen command and receive notification`(): Unit =
        runBlocking {
            PgConnectionHelper.defaultListener().use {
                it.listen(CHANNEL_NAME)
                PgConnectionHelper.defaultConnection().use { conn ->
                    query("select pg_notify('$CHANNEL_NAME', '$PAYLOAD')").execute(conn)
                }

                val notification = withTimeout(1000) { it.receiveNotification() }
                assertEquals(CHANNEL_NAME, notification.channelName)
                assertEquals(PAYLOAD, notification.payload)
            }
        }

    @Test
    fun `listen many should issue a listen command and receive notification`(): Unit =
        runBlocking {
            PgConnectionHelper.defaultListener().use {
                it.listen(CHANNEL_NAME, CHANNEL_NAME2)
                PgConnectionHelper.defaultConnection().use { conn ->
                    query("select pg_notify('$CHANNEL_NAME', '$PAYLOAD')").execute(conn)
                    query("select pg_notify('$CHANNEL_NAME2', '$PAYLOAD')").execute(conn)
                }

                val notification1 = withTimeout(1000) { it.receiveNotification() }
                assertNotNull(notification1)
                assertEquals(CHANNEL_NAME, notification1.channelName)
                assertEquals(PAYLOAD, notification1.payload)

                val notification2 = withTimeout(1000) { it.receiveNotification() }
                assertNotNull(notification2)
                assertEquals(CHANNEL_NAME2, notification2.channelName)
                assertEquals(PAYLOAD, notification2.payload)
            }
        }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `unlisten should remove channels from connections listeners`(unlistenAll: Boolean): Unit =
        runBlocking {
            PgConnectionHelper.defaultListener().use {
                it.listen(CHANNEL_NAME)
                val channelsBefore =
                    preparedQuery(LISTENER_QUERY)
                        .fetchAll(it.connection, StringRowParser)
                assertEquals(1, channelsBefore.size)
                assertEquals(CHANNEL_NAME, channelsBefore[0])

                if (unlistenAll) {
                    it.unlistenAll()
                } else {
                    it.unlisten(CHANNEL_NAME)
                }
                val channelsAfter =
                    preparedQuery(LISTENER_QUERY)
                        .fetchAll(it.connection, StringRowParser)
                assertEquals(0, channelsAfter.size)
            }
        }

    @Test
    fun `notify should issue a notification`(): Unit =
        runBlocking {
            PgConnectionHelper.defaultListener().use {
                it.listen(CHANNEL_NAME)
                PgConnectionHelper.defaultConnection().use { conn ->
                    conn.notify(CHANNEL_NAME, PAYLOAD)
                }
                val notification = withTimeout(1000) { it.receiveNotification() }
                assertEquals(CHANNEL_NAME, notification.channelName)
                assertEquals(PAYLOAD, notification.payload)
            }
        }

    companion object {
        private const val CHANNEL_NAME = "test"
        private const val CHANNEL_NAME2 = "test2"
        private const val PAYLOAD = "This is a test"
        private val LISTENER_QUERY =
            """
            SELECT *
            FROM pg_listening_channels()
            """.trimIndent()
    }
}
