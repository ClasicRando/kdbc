package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.PgTimeTz
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset
import kotlin.test.Test
import kotlin.test.assertEquals

class TestTimeTzType {
    @Test
    fun `encode should accept PgTimeTz when querying postgresql`() = runBlocking {
        val query = "SELECT $1 timetz_col;"

        PgConnectionHelper.defaultSuspendingConnection().use { conn ->
            val value = conn.createPreparedQuery(query)
                .bind(timeTz)
                .fetchScalar<PgTimeTz>()
            assertEquals(expected = timeTz, actual = value)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '05:25:51+02:00'::timetz;"

        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use { conn ->
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<PgTimeTz>()
            assertEquals(timeTz, value)
        }
    }

    @Test
    fun `decode should return PgTimeTz when simple querying postgresql timetz`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return PgTimeTz when extended querying postgresql timetz`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    companion object {
        private val localTime = LocalTime(hour = 5, minute = 25, second = 51)
        private val offset = UtcOffset(hours = 2)
        private val timeTz = PgTimeTz(localTime, offset)
    }
}
