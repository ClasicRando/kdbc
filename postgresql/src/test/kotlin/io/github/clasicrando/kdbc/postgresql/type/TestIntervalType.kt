package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Timeout

class TestIntervalType {
    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept PgInterval when querying postgresql`(): Unit = runBlocking {
        val query = "SELECT $1 interval_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = query(query).bind(pgInterval).fetchScalar<PgInterval>(conn)
            assertEquals(expected = pgInterval, actual = value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return PgInterval when simple querying postgresql interval`(): Unit =
        runBlocking {
            val query =
                "SELECT interval '25 months 14 days 1 hour 1 minute 20 seconds 20 milliseconds 100 microseconds';"
            PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
                val value = query(query).fetchScalar<PgInterval>(conn)
                assertEquals(pgInterval, value)
            }
        }

    companion object {
        private val pgInterval = PgInterval(months = 25, days = 14, microseconds = 3_680_020_100)
    }
}
