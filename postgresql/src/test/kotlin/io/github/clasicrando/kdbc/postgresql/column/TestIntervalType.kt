package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.PgInterval
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.DateTimePeriod
import org.junit.jupiter.api.Timeout
import kotlin.test.Test
import kotlin.test.assertEquals

class TestIntervalType {
    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept DateTimePeriod when querying postgresql`(): Unit = runBlocking {
        val query = "SELECT $1 interval_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = conn.createPreparedQuery(query)
                .bind(dateTimePeriod)
                .fetchScalar<DateTimePeriod>()
            assertEquals(expected = dateTimePeriod, actual = value)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT interval '1 year 4 month 9 day 5 hour 45 minute 8.009005 second';"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<DateTimePeriod>()
            assertEquals(dateTimePeriod, value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return DateTimePeriod when simple querying postgresql interval`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return DateTimePeriod when extended querying postgresql interval`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept PgInterval when querying postgresql`(): Unit = runBlocking {
        val query = "SELECT $1 interval_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = conn.createPreparedQuery(query)
                .bind(pgInterval)
                .fetchScalar<PgInterval>()
            assertEquals(expected = pgInterval, actual = value)
        }
    }

    private suspend fun decodePgIntervalTest(isPrepared: Boolean) {
        val query = "SELECT interval '25 months 14 days 1 hour 1 minute 20 seconds 20 milliseconds 100 microseconds';"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<PgInterval>()
            assertEquals(pgInterval, value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return PgInterval when simple querying postgresql interval`(): Unit = runBlocking {
        decodePgIntervalTest(isPrepared = false)
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return PgInterval when extended querying postgresql interval`(): Unit = runBlocking {
        decodePgIntervalTest(isPrepared = true)
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
        private val pgInterval = PgInterval(
            months = 25,
            days = 14,
            microseconds = 3_680_020_100,
        )
    }
}
