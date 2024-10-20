package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import kotlinx.datetime.toJavaLocalDate
import org.junit.jupiter.api.Timeout
import kotlin.test.Test
import kotlin.test.assertEquals

class TestDateType {
    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept LocalDate when querying postgresql`(): Unit =
        runBlocking {
            val query = "SELECT $1 date_col;"

            PgConnectionHelper.defaultConnection().use { conn ->
                val value =
                    conn.createPreparedQuery(query)
                        .bind(localDate)
                        .fetchScalar<LocalDate>()
                assertEquals(localDate, value)
            }
        }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '2024-02-25'::date;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val value =
                if (isPrepared) {
                    conn.createPreparedQuery(query)
                } else {
                    conn.createQuery(query)
                }.fetchScalar<LocalDate>()
            assertEquals(localDate, value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return LocalDate when simple querying postgresql date`(): Unit =
        runBlocking {
            decodeTest(isPrepared = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return LocalDate when extended querying postgresql date`(): Unit =
        runBlocking {
            decodeTest(isPrepared = true)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept Java LocalDate when querying postgresql`(): Unit =
        runBlocking {
            val query = "SELECT $1 date_col;"

            PgConnectionHelper.defaultConnection().use { conn ->
                val value =
                    conn.createPreparedQuery(query)
                        .bind(javaLocalDate)
                        .fetchScalar<java.time.LocalDate>()
                assertEquals(javaLocalDate, value)
            }
        }

    private suspend fun decodeJavaTest(isPrepared: Boolean) {
        val query = "SELECT '2024-02-25'::date;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val value =
                if (isPrepared) {
                    conn.createPreparedQuery(query)
                } else {
                    conn.createQuery(query)
                }.fetchScalar<java.time.LocalDate>()
            assertEquals(javaLocalDate, value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return Java LocalDate when simple querying postgresql date`(): Unit =
        runBlocking {
            decodeJavaTest(isPrepared = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return Java LocalDate when extended querying postgresql date`(): Unit =
        runBlocking {
            decodeJavaTest(isPrepared = true)
        }

    companion object {
        private val localDate = LocalDate(year = 2024, monthNumber = 2, dayOfMonth = 25)
        private val javaLocalDate = localDate.toJavaLocalDate()
    }
}
