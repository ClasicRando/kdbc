package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalTime
import kotlin.test.Test
import kotlin.test.assertEquals

class TestLocalTimeType {
    @Test
    fun `encode should accept LocalTime when querying postgresql`() = runBlocking {
        val query = "SELECT $1 local_time_col;"

        PgConnectionHelper.defaultSuspendingConnection().use { conn ->
            val value = conn.createPreparedQuery(query)
                .bind(localTime)
                .fetchScalar<LocalTime>()
            assertEquals(expected = localTime, actual = value)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '05:25:51'::time;"

        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use { conn ->
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<LocalTime>()
            assertEquals(localTime, value)
        }
    }

    @Test
    fun `decode should return LocalTime when simple querying postgresql time`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return LocalTime when extended querying postgresql time`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    companion object {
        private val localTime = LocalTime(hour = 5, minute = 25, second = 51)
    }
}
