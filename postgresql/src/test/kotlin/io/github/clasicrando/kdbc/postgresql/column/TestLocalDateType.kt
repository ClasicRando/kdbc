package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals

class TestLocalDateType {
    @Test
    fun `encode should accept LocalDate when querying postgresql`() = runBlocking {
        val localDate = LocalDate(year = 2024, monthNumber = 2, dayOfMonth = 25)
        val query = "SELECT $1 date_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = conn.createPreparedQuery(query)
                .bind(localDate)
                .fetchScalar<LocalDate>()
            assertEquals(localDate, value)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val localDate = LocalDate(year = 2024, monthNumber = 2, dayOfMonth = 25)
        val query = "SELECT '2024-02-25'::date;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<LocalDate>()
            assertEquals(localDate, value)
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
