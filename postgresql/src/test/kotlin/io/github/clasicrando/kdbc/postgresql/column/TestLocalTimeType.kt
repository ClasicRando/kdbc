package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.result.getLocalTime
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalTime
import kotlin.test.Test
import kotlin.test.assertEquals

class TestLocalTimeType {
    @Test
    fun `encode should accept LocalTime when querying postgresql`() = runBlocking {
        val query = "SELECT $1 local_time_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.sendPreparedStatement(query, listOf(localTime)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(
                    expected = localTime,
                    actual = rows.map { it.getLocalTime("local_time_col") }.first(),
                )
            }
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '05:25:51'::time;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            if (isPrepared) {
                conn.sendPreparedStatement(query, emptyList())
            } else {
                conn.sendQuery(query)
            }.use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(localTime, rows.map { it.getLocalTime(0)!! }.first())
            }
        }
    }

    @Test
    fun `decode should return LocalTime when simple querying postgresql time`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return LocalTime when extended querying postgresql time`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    companion object {
        private val localTime = LocalTime(hour = 5, minute = 25, second = 51)
    }
}
