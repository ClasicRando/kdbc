package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.result.getAs
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.DateTimePeriod
import kotlin.test.Test
import kotlin.test.assertEquals

class TestDateTimePeriodType {
    @Test
    fun `encode should accept DateTimePeriod when querying postgresql`() = runBlocking {
        val query = "SELECT $1 interval_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.sendPreparedStatement(query, listOf(dateTimePeriod)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(
                    expected = dateTimePeriod,
                    actual = rows.map { it.getAs<DateTimePeriod>("interval_col") }.first(),
                )
            }
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT interval '1 year 4 month 9 day 5 hour 45 minute 8.009005 second';"

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
                assertEquals(dateTimePeriod, rows.map { it.getAs<DateTimePeriod>(0)!! }.first())
            }
        }
    }

    @Test
    fun `decode should return DateTimePeriod when simple querying postgresql interval`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return DateTimePeriod when extended querying postgresql interval`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    companion object {
        private val dateTimePeriod = DateTimePeriod(
            years = 1,
            months = 4,
            days = 9,
            hours = 5,
            minutes = 45,
            seconds = 8,
            nanoseconds = 9_005_000,
        )
    }
}
