package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
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

        PgConnectionHelper.defaultSuspendingConnection().use { conn ->
            val value = conn.createPreparedQuery(query)
                .bind(dateTimePeriod)
                .fetchScalar<DateTimePeriod>()
            assertEquals(expected = dateTimePeriod, actual = value)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT interval '1 year 4 month 9 day 5 hour 45 minute 8.009005 second';"

        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use { conn ->
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<DateTimePeriod>()
            assertEquals(dateTimePeriod, value)
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
