package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.datetime.DateTime
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset
import kotlinx.datetime.toJavaLocalDate
import kotlinx.datetime.toJavaLocalTime
import kotlinx.datetime.toJavaZoneOffset
import org.junit.jupiter.api.Timeout
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals

class TestTimestampTzType {
    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept DateTime when querying postgresql`(): Unit = runBlocking {
        val query = "SELECT $1 datetime_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = conn.createPreparedQuery(query)
                .bind(dateTime)
                .fetchScalar<DateTime>()
            assertEquals(expected = dateTime.withOffset(UtcOffset.ZERO), actual = value)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '2024-02-25T05:25:51+02'::timestamptz;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<DateTime>()
            assertEquals(dateTime.withOffset(UtcOffset.ZERO), value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return DateTime when simple querying postgresql timestamptz`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return DateTime when extended querying postgresql timestamptz`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }


    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept OffsetDateTime when querying postgresql`(): Unit = runBlocking {
        val query = "SELECT $1 datetime_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = conn.createPreparedQuery(query)
                .bind(offsetDateTime)
                .fetchScalar<OffsetDateTime>()
            assertEquals(
                expected = offsetDateTime.toInstant().atOffset(ZoneOffset.UTC),
                actual = value
            )
        }
    }

    private suspend fun decodeOffsetDateTimeTest(isPrepared: Boolean) {
        val query = "SELECT '2024-02-25T05:25:51+02'::timestamptz;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<OffsetDateTime>()
            assertEquals(
                expected = offsetDateTime.toInstant().atOffset(ZoneOffset.UTC),
                actual = value
            )
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return OffsetDateTime when simple querying postgresql timestamptz`(): Unit = runBlocking {
        decodeOffsetDateTimeTest(isPrepared = false)
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return OffsetDateTime when extended querying postgresql timestamptz`(): Unit = runBlocking {
        decodeOffsetDateTimeTest(isPrepared = true)
    }

    companion object {
        private val localDate = LocalDate(year = 2024, monthNumber = 2, dayOfMonth = 25)
        private val localTime = LocalTime(hour = 5, minute = 25, second = 51)
        private val offset = UtcOffset(hours = 2)
        private val dateTime = DateTime(date = localDate, time = localTime, offset = offset)
        private val offsetDateTime = OffsetDateTime.of(
            localDate.toJavaLocalDate(),
            localTime.toJavaLocalTime(),
            offset.toJavaZoneOffset(),
        )
    }
}
