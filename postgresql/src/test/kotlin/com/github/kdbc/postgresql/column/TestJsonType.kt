package com.github.kdbc.postgresql.column

import com.github.kdbc.core.connection.use
import com.github.kdbc.core.result.getAs
import com.github.kdbc.core.use
import com.github.kdbc.postgresql.PgConnectionHelper
import com.github.kdbc.postgresql.type.PgJson
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals

class TestJsonType {
    @Serializable
    data class JsonType(val number: Double, val text: String)

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `encode should accept PgJson when querying postgresql`(isJsonB: Boolean) = runBlocking {
        val query = "SELECT $1::${if (isJsonB) "jsonb" else "json"} json_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.sendPreparedStatement(query, listOf(pgJsonValue)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                val pgJson = rows.map { it.getAs<PgJson>("json_col")!! }.first()
                assertEquals(jsonValue, pgJson.decode())
            }
        }
    }

    private suspend fun decodeTest(isJsonB: Boolean, isPrepared: Boolean) {
        val query = "SELECT '$JSON_STRING'::${if (isJsonB) "jsonb" else "json"};"

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
                val pgJson = rows.map { it.getAs<PgJson>(0)!! }.first()
                assertEquals(jsonValue, pgJson.decode())
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `decode should return PgJson when simple querying postgresql json`(value: Boolean): Unit = runBlocking {
        decodeTest(isJsonB = value, isPrepared = false)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `decode should return PgJson when extended querying postgresql json`(value: Boolean): Unit = runBlocking {
        decodeTest(isJsonB = value, isPrepared = true)
    }

    companion object {
        private val jsonValue = JsonType(584.5269, "PgJson test")
        private val pgJsonValue = PgJson.fromValue(jsonValue)
        private val JSON_STRING = Json.encodeToString(jsonValue)
    }
}
