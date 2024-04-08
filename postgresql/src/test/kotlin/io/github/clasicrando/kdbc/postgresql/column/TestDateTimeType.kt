package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.datetime.DateTime
import io.github.clasicrando.kdbc.core.result.getDateTime
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset
import kotlin.test.Test
import kotlin.test.assertEquals

class TestDateTimeType {
    @Test
    fun `encode should accept DateTime when querying postgresql`() = runBlocking {
        val query = "SELECT $1 datetime_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.sendPreparedStatement(query, listOf(dateTime)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(
                    expected = dateTime.withOffset(UtcOffset.ZERO),
                    actual = rows.map { it.getDateTime("datetime_col") }.first(),
                )
            }
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '2024-02-25T05:25:51+02'::timestamptz;"

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
                assertEquals(dateTime, rows.map { it.getDateTime(0, offset)!! }.first())
            }
        }
    }

    @Test
    fun `decode should return DateTime when simple querying postgresql timestamptz`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return DateTime when extended querying postgresql timestamptz`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    companion object {
        private val localDate = LocalDate(year = 2024, monthNumber = 2, dayOfMonth = 25)
        private val localTime = LocalTime(hour = 5, minute = 25, second = 51)
        private val offset = UtcOffset(hours = 2)
        private val dateTime = DateTime(date = localDate, time = localTime, offset = offset)
    }
}
