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
import java.time.ZoneOffset
import java.util.stream.Stream
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

class TestTimestampType {
    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @MethodSource("instants")
    fun `encode should accept Instant when querying postgresql`(instant: Instant): Unit =
        runBlocking {
            val query = "SELECT $1 instant_col;"

            PgConnectionHelper.defaultConnection().use { conn ->
                val value = query(query).bind(instant).fetchScalar<Instant>(conn)
                assertEquals(expected = instant, actual = value)
            }
        }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @MethodSource("instants")
    fun `decode should return Instant when simple querying postgresql timestamp`(
        instant: Instant
    ): Unit = runBlocking {
        val query = "SELECT '$instant'::timestamp;"
        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val value = query(query).fetchScalar<Instant>(conn)
            assertEquals(instant, value)
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @MethodSource("localDateTimes")
    fun `encode should accept LocalDateTime when querying postgresql`(
        localDateTime: LocalDateTime
    ): Unit = runBlocking {
        val query = "SELECT $1 instant_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = query(query).bind(localDateTime).fetchScalar<LocalDateTime>(conn)
            assertEquals(expected = localDateTime, actual = value)
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @MethodSource("localDateTimes")
    fun `decode should return Java LocalDateTime when simple querying postgresql timestamp`(
        localDateTime: LocalDateTime
    ): Unit = runBlocking {
        val query = "SELECT '$localDateTime'::timestamp;"
        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val value = query(query).fetchScalar<LocalDateTime>(conn)
            assertEquals(localDateTime, value)
        }
    }

    companion object {
        private val positiveLocalDate = LocalDate.of(2024, 2, 25)
        private val positiveLocalTime = LocalTime.of(5, 25, 51)
        private val positiveLocalDateTime = LocalDateTime.of(positiveLocalDate, positiveLocalTime)
        private val positiveInstant = positiveLocalDateTime.toInstant(ZoneOffset.UTC)

        private val negativeLocalDate = LocalDate.of(1990, 8, 3)
        private val negativeLocalTime = LocalTime.of(13, 56, 8)
        private val negativeLocalDateTime = LocalDateTime.of(negativeLocalDate, negativeLocalTime)
        private val negativeInstant = negativeLocalDateTime.toInstant(ZoneOffset.UTC)

        @JvmStatic
        private fun instants(): Stream<Instant> = listOf(positiveInstant, negativeInstant).stream()

        @JvmStatic
        private fun localDateTimes(): Stream<LocalDateTime> =
            listOf(positiveLocalDateTime, negativeLocalDateTime).stream()
    }
}
