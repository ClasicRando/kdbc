package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.common.result.getBoolean
import com.github.clasicrando.common.use
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

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.sendPreparedStatement(query, listOf(value)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(value, rows.map { it.getBoolean("bool_col") }.first())
            }
        }
    }

    private suspend fun decodeTest(value: Boolean, isPrepared: Boolean) {
        val query = "SELECT $value;"

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
                assertEquals(value, rows.map { it.getBoolean(0) }.first())
            }
        }
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