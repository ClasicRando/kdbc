package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import java.time.LocalTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Timeout

class TestTimeType {
    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept LocalTime when querying postgresql`(): Unit = runBlocking {
        val query = "SELECT $1 local_time_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = query(query).bind(localTime).fetchScalar<LocalTime>(conn)
            assertEquals(expected = localTime, actual = value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return LocalTime when simple querying postgresql time`(): Unit =
        runBlocking {
            val query = "SELECT '05:25:51'::time;"
            PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
                val value = query(query).fetchScalar<LocalTime>(conn)
                assertEquals(localTime, value)
            }
        }

    companion object {
        private val localTime = LocalTime.of(5, 25, 51)
    }
}
