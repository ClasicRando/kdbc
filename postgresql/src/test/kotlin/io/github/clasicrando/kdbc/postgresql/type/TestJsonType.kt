package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.type.Json
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json as KotlinxJson
import kotlinx.serialization.json.encodeToJsonElement
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class TestJsonType {
    @Serializable data class JsonType(val number: Double, val text: String)

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(booleans = [true, false])
    fun `encode should accept Json when querying postgresql`(isJsonB: Boolean): Unit = runBlocking {
        val tableName = if (isJsonB) JSONB_TEST_TABLE else JSON_TEST_TABLE
        val query = "INSERT INTO public.$tableName(column_1) VALUES($1) RETURNING column_1"

        PgConnectionHelper.defaultConnection().use { conn ->
            val pgJson = query(query).bind(pgJsonValue).fetchScalar<Json>(conn)
            assertNotNull(pgJson)
            assertEquals(jsonValue, pgJson.decodeUsingSerialization())
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(booleans = [true, false])
    fun `decode should return Json when simple querying postgresql json`(isJsonB: Boolean): Unit =
        runBlocking {
            val query = "SELECT '$JSON_STRING'::${if (isJsonB) "jsonb" else "json"};"
            PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
                val pgJson = query(query).fetchScalar<Json>(conn)
                assertNotNull(pgJson)
                assertEquals(jsonValue, pgJson.decodeUsingSerialization())
            }
        }

    companion object {
        private val jsonValue = JsonType(584.5269, "PgJson test")
        private val pgJsonValue = Json.fromJsonElement(KotlinxJson.encodeToJsonElement(jsonValue))
        private val JSON_STRING = KotlinxJson.encodeToString(jsonValue)
        private const val JSON_TEST_TABLE = "json_test"
        private const val JSONB_TEST_TABLE = "jsonb_test"

        @BeforeAll
        @JvmStatic
        fun createObjects(): Unit = runBlocking {
            PgConnectionHelper.defaultConnection().use {
                query("DROP TABLE IF EXISTS public.$JSON_TEST_TABLE").execute(it)
                query("DROP TABLE IF EXISTS public.$JSONB_TEST_TABLE").execute(it)
                query("CREATE TABLE public.$JSON_TEST_TABLE(column_1 json)").execute(it)
                query("CREATE TABLE public.$JSONB_TEST_TABLE(column_1 jsonb)").execute(it)
            }
        }

        @AfterAll
        @JvmStatic
        fun cleanObjects(): Unit = runBlocking {
            PgConnectionHelper.defaultConnection().use {
                query("DROP TABLE IF EXISTS public.$JSON_TEST_TABLE").execute(it)
                query("DROP TABLE IF EXISTS public.$JSONB_TEST_TABLE").execute(it)
            }
        }
    }
}
