package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.common.result.getAs
import com.github.clasicrando.common.result.getBoolean
import com.github.clasicrando.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals

class TestBooleanType {
    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `encode should accept Boolean when querying postgresql`(value: Boolean) = runBlocking {
        val query = "SELECT $1 bool_col;"

        val result = PgConnectionHelper.defaultConnection().use {
            it.sendPreparedStatement(query, listOf(value))
        }.toList()

        assertEquals(1, result.size)
        assertEquals(1, result[0].rowsAffected)
        val rows = result[0].rows.toList()
        assertEquals(1, rows.size)
        assertEquals(value, rows.map { it.getAs<Boolean>("bool_col") }.first())
    }

    private suspend fun decodeTest(value: Boolean, isPrepared: Boolean) {
        val query = "SELECT $value;"

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
        assertEquals(value, rows.map { it.getBoolean(0) }.first())
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `decode should return Boolean when simple querying postgresql bool`(value: Boolean): Unit = runBlocking {
        decodeTest(value = value, isPrepared = false)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `decode should return Boolean when extended querying postgresql bool`(value: Boolean): Unit = runBlocking {
        decodeTest(value = value, isPrepared = true)
    }
}