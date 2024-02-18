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
        val query = "SELECT $1;"

        val result = PgConnectionHelper.defaultConnection().use {
            it.sendPreparedStatement(query, listOf(value))
        }.toList()

        assertEquals(1, result.size)
        assertEquals(1, result[0].rowsAffected)
        val rows = result[0].rows.toList()
        assertEquals(1, rows.size)
        assertEquals(value, rows.map { it.getAs<BigDecimal>(0) }.first())
    }

    private suspend inline fun `decode should return int list when querying postgresql int array`(
        isPrepared: Boolean,
    ) {
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
    fun `decode should return int list when simple querying postgresql int array`(): Unit = runBlocking {
        `decode should return int list when querying postgresql int array`(isPrepared = false)
    }

    @Test
    fun `decode should return int list when extended querying postgresql int array`(): Unit = runBlocking {
        `decode should return int list when querying postgresql int array`(isPrepared = true)
    }
}
