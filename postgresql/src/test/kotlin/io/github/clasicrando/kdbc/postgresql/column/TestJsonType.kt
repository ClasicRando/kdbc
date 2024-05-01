package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.PgJson
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class TestJsonType {
    @Serializable
    data class JsonType(val number: Double, val text: String)

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `encode should accept PgJson when querying postgresql`(isJsonB: Boolean) = runBlocking {
        val query = "SELECT $1::${if (isJsonB) "jsonb" else "json"} json_col;"

        PgConnectionHelper.defaultSuspendingConnection().use { conn ->
            val pgJson = conn.createPreparedQuery(query)
                .bind(pgJsonValue)
                .fetchScalar<PgJson>()
            assertNotNull(pgJson)
            assertEquals(jsonValue, pgJson.decode())
        }
    }

    private suspend fun decodeTest(isJsonB: Boolean, isPrepared: Boolean) {
        val query = "SELECT '$JSON_STRING'::${if (isJsonB) "jsonb" else "json"};"

        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use { conn ->
            val pgJson = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<PgJson>()
            assertNotNull(pgJson)
            assertEquals(jsonValue, pgJson.decode())
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
