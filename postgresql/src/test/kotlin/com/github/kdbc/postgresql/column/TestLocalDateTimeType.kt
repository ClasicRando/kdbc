package com.github.kdbc.postgresql.column

import com.github.kdbc.core.connection.use
import com.github.kdbc.core.result.getLocalDateTime
import com.github.kdbc.core.use
import com.github.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import kotlin.test.Test
import kotlin.test.assertEquals

class TestLocalDateTimeType {
    @Test
    fun `encode should accept LocalDateTime when querying postgresql`() = runBlocking {
        val query = "SELECT $1 local_datetime_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.sendPreparedStatement(query, listOf(localDateTime)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(
                    expected = localDateTime,
                    actual = rows.map { it.getLocalDateTime("local_datetime_col") }.first(),
                )
            }
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '2024-02-25T05:25:51'::timestamp;"

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
                assertEquals(localDateTime, rows.map { it.getLocalDateTime(0)!! }.first())
            }
        }
    }

    @Test
    fun `decode should return LocalDateTime when simple querying postgresql timestamp`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return LocalDateTime when extended querying postgresql timestamp`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    companion object {
        private val localDate = LocalDate(year = 2024, monthNumber = 2, dayOfMonth = 25)
        private val localTime = LocalTime(hour = 5, minute = 25, second = 51)
        private val localDateTime = LocalDateTime(localDate, localTime)
    }
}
