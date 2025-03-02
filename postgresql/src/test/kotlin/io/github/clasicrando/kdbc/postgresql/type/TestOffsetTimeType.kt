package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Timeout

class TestOffsetTimeType {
    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept OffsetTime when querying postgresql`(): Unit = runBlocking {
        val query = "SELECT $1 timetz_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = query(query).bind(offsetTime).fetchScalar<OffsetTime>(conn)
            assertEquals(expected = offsetTime, actual = value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return OffsetTime when simple querying postgresql timetz`(): Unit =
        runBlocking {
            val query = "SELECT '05:25:51+02:00'::timetz;"
            PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
                val value = query(query).fetchScalar<OffsetTime>(conn)
                assertEquals(offsetTime, value)
            }
        }

    companion object {
        private val localTime = LocalTime.of(5, 25, 51)
        private val offsetTime = OffsetTime.of(localTime, ZoneOffset.ofHours(2))
    }
}
