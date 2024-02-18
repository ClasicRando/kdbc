package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.common.result.getAs
import com.github.clasicrando.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import java.math.BigDecimal
import kotlin.test.Test
import kotlin.test.assertEquals

class TestBigDecimalType {
    @Test
    fun `encode should accept BiDecimal when querying postgresql`() = runBlocking {
        val value = BigDecimal("2548.52489")
        val query = "SELECT $1 numeric_col;"

        val result = PgConnectionHelper.defaultConnection().use {
            it.sendPreparedStatement(query, listOf(value))
        }.toList()

        assertEquals(1, result.size)
        assertEquals(1, result[0].rowsAffected)
        val rows = result[0].rows.toList()
        assertEquals(1, rows.size)
        assertEquals(value, rows.map { it.getAs<BigDecimal>("numeric_col") }.first())
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val number = "2548.52489"
        val expectedResult = BigDecimal(number)
        val query = "SELECT $number;"

        val result = PgConnectionHelper.defaultConnectionWithForcedSimple().use {
            if (isPrepared) {
                it.sendPreparedStatement(query, emptyList())
            } else {
                it.sendQuery(query)
            }
        }.toList()

        assertEquals(1, result.size)
        assertEquals(1, result[0].rowsAffected)
        val rows = result[0].rows.toList()
        assertEquals(1, rows.size)
        assertEquals(expectedResult, rows.map { it.getAs<BigDecimal>(0) }.first())
    }

    @Test
    fun `decode should return BigDecimal when simple querying postgresql numeric`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return BigDecimal when extended querying postgresql numeric`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }
}