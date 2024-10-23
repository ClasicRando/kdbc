package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalTime
import kotlinx.datetime.toJavaLocalTime
import org.junit.jupiter.api.Timeout
import kotlin.test.Test
import kotlin.test.assertEquals

class TestTimeType {
    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept LocalTime when querying postgresql`(): Unit =
        runBlocking {
            val query = "SELECT $1 local_time_col;"

            PgConnectionHelper.defaultConnection().use { conn ->
                val value =
                    query(query)
                        .bind(localTime)
                        .fetchScalar<LocalTime>(conn)
                assertEquals(expected = localTime, actual = value)
            }
        }

    private suspend fun decodeTest(isExtended: Boolean) {
        val query = "SELECT '05:25:51'::time;"
        if (isExtended) {
            PgConnectionHelper.defaultConnection()
        } else {
            PgConnectionHelper.defaultConnectionWithForcedSimple()
        }.use { conn ->
            val value = query(query).fetchScalar<LocalTime>(conn)
            assertEquals(localTime, value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return LocalTime when simple querying postgresql time`(): Unit =
        runBlocking {
            decodeTest(isExtended = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return LocalTime when extended querying postgresql time`(): Unit =
        runBlocking {
            decodeTest(isExtended = true)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept Java LocalTime when querying postgresql`(): Unit =
        runBlocking {
            val query = "SELECT $1 local_time_col;"

            PgConnectionHelper.defaultConnection().use { conn ->
                val value =
                    query(query)
                        .bind(javaLocalTime)
                        .fetchScalar<java.time.LocalTime>(conn)
                assertEquals(expected = javaLocalTime, actual = value)
            }
        }

    private suspend fun decodeJavaTest(isExtended: Boolean) {
        val query = "SELECT '05:25:51'::time;"
        if (isExtended) {
            PgConnectionHelper.defaultConnection()
        } else {
            PgConnectionHelper.defaultConnectionWithForcedSimple()
        }.use { conn ->
            val value = query(query).fetchScalar<java.time.LocalTime>(conn)
            assertEquals(javaLocalTime, value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return Java LocalTime when simple querying postgresql time`(): Unit =
        runBlocking {
            decodeJavaTest(isExtended = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return Java LocalTime when extended querying postgresql time`(): Unit =
        runBlocking {
            decodeJavaTest(isExtended = true)
        }

    companion object {
        private val localTime = LocalTime(hour = 5, minute = 25, second = 51)
        private val javaLocalTime = localTime.toJavaLocalTime()
    }
}
