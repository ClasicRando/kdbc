package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
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
            val value = conn.createPreparedQuery(query)
                .bind(localDateTime)
                .fetchScalar<LocalDateTime>()
            assertEquals(expected = localDateTime, actual = value)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT '2024-02-25T05:25:51'::timestamp;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<LocalDateTime>()
            assertEquals(localDateTime, value)
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
