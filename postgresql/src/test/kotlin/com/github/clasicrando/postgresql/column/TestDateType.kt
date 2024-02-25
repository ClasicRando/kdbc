package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.common.result.getLocalDate
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals

class TestDateType {
    @Test
    fun `encode should accept CompositeTest when querying postgresql`() = runBlocking {
        val localDate = LocalDate(year = 2024, monthNumber = 2, dayOfMonth = 25)
        val query = "SELECT $1 date_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.sendPreparedStatement(query, listOf(localDate)).use { results ->
                val result = results.toList()

                assertEquals(1, result.size)
                assertEquals(1, result[0].rowsAffected)
                val rows = result[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(localDate, rows.map { it.getLocalDate("date_col") }.first())
            }
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val localDate = LocalDate(year = 2024, monthNumber = 2, dayOfMonth = 25)
        val query = "SELECT '2024-02-25'::date;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            if (isPrepared) {
                conn.sendPreparedStatement(query, emptyList())
            } else {
                conn.sendQuery(query)
            }.use { results ->
                val result = results.toList()

                assertEquals(1, result.size)
                assertEquals(1, result[0].rowsAffected)
                val rows = result[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(localDate, rows.map { it.getLocalDate(0)!! }.first())
            }
        }
    }

    @Test
    fun `decode should return CompositeType when simple querying postgresql composite`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return CompositeType when extended querying postgresql composite`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }
}
