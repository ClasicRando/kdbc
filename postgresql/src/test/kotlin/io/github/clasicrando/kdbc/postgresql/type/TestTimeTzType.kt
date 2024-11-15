package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import java.time.OffsetTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset
import kotlinx.datetime.toJavaLocalTime
import kotlinx.datetime.toJavaZoneOffset
import org.junit.jupiter.api.Timeout

class TestTimeTzType {
    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept PgTimeTz when querying postgresql`(): Unit = runBlocking {
        val query = "SELECT $1 timetz_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = query(query).bind(timeTz).fetchScalar<PgTimeTz>(conn)
            assertEquals(expected = timeTz, actual = value)
        }
    }

    private suspend fun decodeTest(isExtended: Boolean) {
        val query = "SELECT '05:25:51+02:00'::timetz;"
        if (isExtended) {
                PgConnectionHelper.defaultConnection()
            } else {
                PgConnectionHelper.defaultConnectionWithForcedSimple()
            }
            .use { conn ->
                val value = query(query).fetchScalar<PgTimeTz>(conn)
                assertEquals(timeTz, value)
            }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return PgTimeTz when simple querying postgresql timetz`(): Unit =
        runBlocking {
            decodeTest(isExtended = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return PgTimeTz when extended querying postgresql timetz`(): Unit =
        runBlocking {
            decodeTest(isExtended = true)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept OffsetTime when querying postgresql`(): Unit = runBlocking {
        val query = "SELECT $1 timetz_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = query(query).bind(offsetTime).fetchScalar<OffsetTime>(conn)
            assertEquals(expected = offsetTime, actual = value)
        }
    }

    private suspend fun decodeOffsetTimeTest(isExtended: Boolean) {
        val query = "SELECT '05:25:51+02:00'::timetz;"
        if (isExtended) {
                PgConnectionHelper.defaultConnection()
            } else {
                PgConnectionHelper.defaultConnectionWithForcedSimple()
            }
            .use { conn ->
                val value = query(query).fetchScalar<OffsetTime>(conn)
                assertEquals(offsetTime, value)
            }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return OffsetTime when simple querying postgresql timetz`(): Unit =
        runBlocking {
            decodeOffsetTimeTest(isExtended = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return OffsetTime when extended querying postgresql timetz`(): Unit =
        runBlocking {
            decodeOffsetTimeTest(isExtended = true)
        }

    companion object {
        private val localTime = LocalTime(hour = 5, minute = 25, second = 51)
        private val offset = UtcOffset(hours = 2)
        private val timeTz = PgTimeTz(localTime, offset)
        private val offsetTime =
            OffsetTime.of(localTime.toJavaLocalTime(), offset.toJavaZoneOffset())
    }
}
