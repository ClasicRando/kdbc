package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Timeout

class TestDateType {
    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept LocalDate when querying postgresql`(): Unit = runBlocking {
        val query = "SELECT $1 date_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = query(query).bind(localDate).fetchScalar<LocalDate>(conn)
            assertEquals(localDate, value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return LocalDate when simple querying postgresql date`(): Unit =
        runBlocking {
            val query = "SELECT '2024-02-25'::date;"
            PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
                val value = query(query).fetchScalar<LocalDate>(conn)
                assertEquals(localDate, value)
            }
        }

    companion object {
        private val localDate = LocalDate.of(2024, 2, 25)
    }
}
