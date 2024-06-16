package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.Instant
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset
import kotlinx.datetime.toInstant
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream
import kotlin.test.assertEquals

class TestInstantType {
    @ParameterizedTest
    @MethodSource("instants")
    fun `encode should accept Instant when querying postgresql`(instant: Instant) = runBlocking {
        val query = "SELECT $1 instant_col;"

        PgConnectionHelper.defaultSuspendingConnection().use { conn ->
            val value = conn.createPreparedQuery(query)
                .bind(instant)
                .fetchScalar<Instant>()
            assertEquals(expected = instant, actual = value)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean, expectedValue: Instant) {
        val query = "SELECT '$expectedValue'::timestamp;"

        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use { conn ->
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<Instant>()
            assertEquals(expectedValue, value)
        }
    }

    @ParameterizedTest
    @MethodSource("instants")
    fun `decode should return Instant when simple querying postgresql timestamp`(instant: Instant): Unit = runBlocking {
        decodeTest(isPrepared = false, expectedValue = instant)
    }

    @ParameterizedTest
    @MethodSource("instants")
    fun `decode should return Instant when extended querying postgresql timestamp`(instant: Instant): Unit = runBlocking {
        decodeTest(isPrepared = true, expectedValue = instant)
    }

    companion object {
        private val positiveLocalDate = LocalDate(year = 2024, monthNumber = 2, dayOfMonth = 25)
        private val positiveLocalTime = LocalTime(hour = 5, minute = 25, second = 51)
        private val positiveLocalDateTime = LocalDateTime(positiveLocalDate, positiveLocalTime)
        private val positiveInstant = positiveLocalDateTime.toInstant(UtcOffset.ZERO)

        private val negativeLocalDate = LocalDate(year = 1990, monthNumber = 8, dayOfMonth = 3)
        private val negativeLocalTime = LocalTime(hour = 13, minute = 56, second = 8)
        private val negativeLocalDateTime = LocalDateTime(negativeLocalDate, negativeLocalTime)
        private val negativeInstant = negativeLocalDateTime.toInstant(UtcOffset.ZERO)

        @JvmStatic
        private fun instants(): Stream<Instant> {
            return listOf(positiveInstant, negativeInstant).stream()
        }
    }
}
