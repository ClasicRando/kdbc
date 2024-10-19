package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.PgTimeTz
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset
import kotlinx.datetime.toJavaLocalTime
import kotlinx.datetime.toJavaZoneOffset
import java.time.OffsetTime
import kotlin.test.Test
import kotlin.test.assertEquals

class TestTimeTzType {
    @Test
    fun `encode should accept PgTimeTz when querying postgresql`() = runBlocking {
        val query = "SELECT $1 timetz_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = conn.createPreparedQuery(query)
                .bind(timeTz)
                .fetchScalar<PgTimeTz>()
            assertEquals(expected = timeTz, actual = value)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '05:25:51+02:00'::timetz;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
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

    @Test
    fun `encode should accept OffsetTime when querying postgresql`() = runBlocking {
        val query = "SELECT $1 timetz_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = conn.createPreparedQuery(query)
                .bind(offsetTime)
                .fetchScalar<OffsetTime>()
            assertEquals(expected = offsetTime, actual = value)
        }
    }

    private suspend fun decodeOffsetTimeTest(isPrepared: Boolean) {
        val query = "SELECT '05:25:51+02:00'::timetz;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<OffsetTime>()
            assertEquals(offsetTime, value)
        }
    }

    @Test
    fun `decode should return OffsetTime when simple querying postgresql timetz`(): Unit = runBlocking {
        decodeOffsetTimeTest(isPrepared = false)
    }

    @Test
    fun `decode should return OffsetTime when extended querying postgresql timetz`(): Unit = runBlocking {
        decodeOffsetTimeTest(isPrepared = true)
    }

    companion object {
        private val localTime = LocalTime(hour = 5, minute = 25, second = 51)
        private val offset = UtcOffset(hours = 2)
        private val timeTz = PgTimeTz(localTime, offset)
        private val offsetTime = OffsetTime.of(
            localTime.toJavaLocalTime(),
            offset.toJavaZoneOffset(),
        )
    }
}
