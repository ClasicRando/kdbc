package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.result.getLocalDate
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals

class TestDateType {
    @Test
    fun `encode should accept LocalDate when querying postgresql`() = runBlocking {
        val localDate = LocalDate(year = 2024, monthNumber = 2, dayOfMonth = 25)
        val query = "SELECT $1 date_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.sendPreparedStatement(query, listOf(localDate)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
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
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(localDate, rows.map { it.getLocalDate(0)!! }.first())
            }
        }
    }

    @Test
    fun `decode should return LocalDate when simple querying postgresql date`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return LocalDate when extended querying postgresql date`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }
}
