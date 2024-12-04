package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.mysql.MySqlConnectionHelper
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.time.Duration
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class TestChrono {
    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["13:18:13", "05:23:59"])
    fun `encode should accept LocalTime`(value: String) {
        val expected = LocalTime.parse(value)
        runBlocking {
            val query = "SELECT ? time_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(expected).fetchScalar<LocalTime>(conn)
                Assertions.assertEquals(expected, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["13:18:13", "05:23:59"])
    fun `decode should return LocalTime when text`(value: String) {
        val expected = LocalTime.parse(value)
        runBlocking {
            val query = "SELECT TIME '$value' time_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<LocalTime>(conn)
                Assertions.assertEquals(expected, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["PT830H18M13S"])
    fun `encode should accept Duration`(value: String) {
        val expected = Duration.parse(value)
        runBlocking {
            val query = "SELECT ? duration_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(expected).fetchScalar<Duration>(conn)
                Assertions.assertEquals(expected, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["PT830H18M13S"])
    fun `decode should return Duration when text`(value: String) {
        val expected = Duration.parse(value)
        runBlocking {
            val timeStr =
                expected.toComponents { hour, minutes, seconds, nanoseconds ->
                    "$hour:$minutes:$seconds.$nanoseconds"
                }
            val query = "SELECT TIME '$timeStr' duration_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<Duration>(conn)
                Assertions.assertEquals(expected, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["2024-01-01"])
    fun `encode should accept LocalDate`(value: String) {
        val expected = LocalDate.parse(value)
        runBlocking {
            val query = "SELECT ? date_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(expected).fetchScalar<LocalDate>(conn)
                Assertions.assertEquals(expected, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["2024-01-01"])
    fun `decode should return LocalDate when text`(value: String) {
        val expected = LocalDate.parse(value)
        runBlocking {
            val query = "SELECT DATE '$value' date_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<LocalDate>(conn)
                Assertions.assertEquals(expected, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["2024-01-01T05:24:57"])
    fun `encode should accept LocalDateTime`(value: String) {
        val expected = LocalDateTime.parse(value)
        runBlocking {
            val query = "SELECT ? timestamp_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).bind(expected).fetchScalar<LocalDateTime>(conn)
                Assertions.assertEquals(expected, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["2024-01-01T05:24:57"])
    fun `decode should return LocalDateTime when text`(value: String) {
        val expected = LocalDateTime.parse(value)
        runBlocking {
            val query = "SELECT TIMESTAMP '$value' timestamp_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual = query(query).fetchScalar<LocalDateTime>(conn)
                Assertions.assertEquals(expected, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["2024-01-01T05:24:57+05:00"])
    fun `encode should accept OffsetDateTime`(value: String) {
        val expected = OffsetDateTime.parse(value)
        runBlocking {
            val query = "SELECT ? timestamp_with_timezone_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual =
                    query(query)
                        .bind(expected)
                        .fetchScalar<OffsetDateTime>(conn)
                        ?.withOffsetSameInstant(expected.offset)
                Assertions.assertEquals(expected, actual)
            }
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(strings = ["2024-01-01T05:24:57+05:00"])
    fun `decode should return OffsetDateTime when text`(value: String) {
        val expected = OffsetDateTime.parse(value)
        val expectedLocalDateTime = expected.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime()
        runBlocking {
            val query = "SELECT TIMESTAMP '$expectedLocalDateTime' timestamp_with_timezone_column;"

            MySqlConnectionHelper.defaultConnection().use { conn ->
                val actual =
                    query(query)
                        .fetchScalar<OffsetDateTime>(conn)
                        ?.withOffsetSameInstant(expected.offset)
                Assertions.assertEquals(expected, actual)
            }
        }
    }
}
