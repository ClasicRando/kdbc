package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.executeClosing
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.PgJson
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToJsonElement
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
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
        val tableName = if (isJsonB) JSONB_TEST_TABLE else JSON_TEST_TABLE
        val query = "INSERT INTO public.$tableName(column_1) VALUES($1) RETURNING column_1"

        PgConnectionHelper.defaultAsyncConnection().use { conn ->
            val pgJson = conn.createPreparedQuery(query)
                .bind(pgJsonValue)
                .fetchScalar<PgJson>()
            assertNotNull(pgJson)
            assertEquals(jsonValue, pgJson.decodeUsingSerialization())
        }
    }

    private suspend fun decodeTest(isJsonB: Boolean, isPrepared: Boolean) {
        val query = "SELECT '$JSON_STRING'::${if (isJsonB) "jsonb" else "json"};"

        PgConnectionHelper.defaultAsyncConnectionWithForcedSimple().use { conn ->
            val pgJson = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<PgJson>()
            assertNotNull(pgJson)
            assertEquals(jsonValue, pgJson.decodeUsingSerialization())
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
        private val pgJsonValue = PgJson(Json.encodeToJsonElement(jsonValue))
        private val JSON_STRING = Json.encodeToString(jsonValue)
        private const val JSON_TEST_TABLE = "json_test"
        private const val JSONB_TEST_TABLE = "jsonb_test"

        @BeforeAll
        @JvmStatic
        fun createObjects() {
            PgConnectionHelper.defaultBlockingConnection().use {
                it.createQuery("DROP TABLE IF EXISTS public.$JSON_TEST_TABLE").executeClosing()
                it.createQuery("DROP TABLE IF EXISTS public.$JSONB_TEST_TABLE").executeClosing()
                it.createQuery("CREATE TABLE public.$JSON_TEST_TABLE(column_1 json)").executeClosing()
                it.createQuery("CREATE TABLE public.$JSONB_TEST_TABLE(column_1 jsonb)").executeClosing()
            }
        }

        @AfterAll
        @JvmStatic
        fun cleanObjects() {
            PgConnectionHelper.defaultBlockingConnection().use {
                it.createQuery("DROP TABLE IF EXISTS public.$JSON_TEST_TABLE").executeClosing()
                it.createQuery("DROP TABLE IF EXISTS public.$JSONB_TEST_TABLE").executeClosing()
            }
        }
    }
}
