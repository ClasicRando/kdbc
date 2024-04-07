package com.github.kdbc.postgresql.column

import com.github.kdbc.core.connection.use
import com.github.kdbc.core.result.getAs
import com.github.kdbc.core.use
import com.github.kdbc.postgresql.PgConnectionHelper
import com.github.kdbc.postgresql.type.PgTimeTz
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset
import kotlin.test.Test
import kotlin.test.assertEquals

class TestTimeTzType {
    @Test
    fun `encode should accept PgTimeTz when querying postgresql`() = runBlocking {
        val query = "SELECT $1 timetz_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.sendPreparedStatement(query, listOf(timeTz)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(
                    expected = timeTz,
                    actual = rows.map { it.getAs<PgTimeTz>("timetz_col") }.first(),
                )
            }
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '05:25:51+02:00'::timetz;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            if (isPrepared) {
                conn.sendPreparedStatement(query, emptyList())
            } else {
                conn.sendQuery(query)
            }.use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                val value = rows.map { it.getAs<PgTimeTz>(0)!! }.first()
                assertEquals(timeTz, value)
            }
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
