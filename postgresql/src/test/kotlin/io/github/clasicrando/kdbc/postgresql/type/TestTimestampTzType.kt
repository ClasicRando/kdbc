package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Timeout

class TestTimestampTzType {
    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept Instant when querying postgresql`(): Unit = runBlocking {
        val query = "SELECT $1 instant_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = query(query).bind(instant).fetchScalar<Instant>(conn)
            assertEquals(expected = instant, actual = value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return DateTime when simple querying postgresql timestamptz`(): Unit =
        runBlocking {
            val query = "SELECT '2024-02-25T05:25:51+02'::timestamptz;"
            PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
                val value = query(query).fetchScalar<Instant>(conn)
                assertEquals(instant, value)
            }
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept OffsetDateTime when querying postgresql`(): Unit = runBlocking {
        val query = "SELECT $1 datetime_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = query(query).bind(offsetDateTime).fetchScalar<OffsetDateTime>(conn)
            assertEquals(
                expected = offsetDateTime.toInstant().atOffset(ZoneOffset.UTC),
                actual = value,
            )
        }
    }

    private suspend fun decodeOffsetDateTimeTest(isExtended: Boolean) {
        val query = "SELECT '2024-02-25T05:25:51+02'::timestamptz;"
        if (isExtended) {
                PgConnectionHelper.defaultConnection()
            } else {
                PgConnectionHelper.defaultConnectionWithForcedSimple()
            }
            .use { conn ->
                val value = query(query).fetchScalar<OffsetDateTime>(conn)
                assertEquals(
                    expected = offsetDateTime.toInstant().atOffset(ZoneOffset.UTC),
                    actual = value,
                )
            }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return OffsetDateTime when simple querying postgresql timestamptz`(): Unit =
        runBlocking {
            decodeOffsetDateTimeTest(isExtended = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return OffsetDateTime when extended querying postgresql timestamptz`():
        Unit = runBlocking { decodeOffsetDateTimeTest(isExtended = true) }

    companion object {
        private val localDate = LocalDate.of(2024, 2, 25)
        private val localTime = LocalTime.of(5, 25, 51)
        private val offset = ZoneOffset.ofHours(2)
        private val instant = LocalDateTime.of(localDate, localTime).toInstant(offset)
        private val offsetDateTime = OffsetDateTime.of(localDate, localTime, offset)
    }
}
