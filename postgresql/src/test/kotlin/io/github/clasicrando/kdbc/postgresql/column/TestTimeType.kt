package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
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
    fun `encode should accept LocalTime when querying postgresql`(): Unit = runBlocking {
        val query = "SELECT $1 local_time_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = conn.createPreparedQuery(query)
                .bind(localTime)
                .fetchScalar<LocalTime>()
            assertEquals(expected = localTime, actual = value)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '05:25:51'::time;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<LocalTime>()
            assertEquals(localTime, value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return LocalTime when simple querying postgresql time`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return LocalTime when extended querying postgresql time`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept Java LocalTime when querying postgresql`(): Unit = runBlocking {
        val query = "SELECT $1 local_time_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = conn.createPreparedQuery(query)
                .bind(javaLocalTime)
                .fetchScalar<java.time.LocalTime>()
            assertEquals(expected = javaLocalTime, actual = value)
        }
    }

    private suspend fun decodeJavaTest(isPrepared: Boolean) {
        val query = "SELECT '05:25:51'::time;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<java.time.LocalTime>()
            assertEquals(javaLocalTime, value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return Java LocalTime when simple querying postgresql time`(): Unit = runBlocking {
        decodeJavaTest(isPrepared = false)
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return Java LocalTime when extended querying postgresql time`(): Unit = runBlocking {
        decodeJavaTest(isPrepared = true)
    }

    companion object {
        private val localTime = LocalTime(hour = 5, minute = 25, second = 51)
        private val javaLocalTime = localTime.toJavaLocalTime()
    }
}
