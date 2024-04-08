package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.result.getByte
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.Test
import kotlin.test.assertEquals

class TestCharType {
    @ParameterizedTest
    @ValueSource(bytes = [0, 2, 58, 122])
    fun `encode should accept Byte when querying postgresql`(value: Byte) = runBlocking {
        val query = "SELECT $1 char_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.sendPreparedStatement(query, listOf(value)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(value, rows.map { it.getByte("char_col") }.first())
            }
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT char_field FROM char_test ORDER BY char_field;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            if (isPrepared) {
                conn.sendPreparedStatement(query, emptyList())
            } else {
                conn.sendQuery(query)
            }.use { results ->
                assertEquals(1, results.size)
                assertEquals(3, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(3, rows.size)
                Assertions.assertArrayEquals(bytes, rows.map { it.getByte(0)!! }.toByteArray())
            }
        }
    }

    @Test
    fun `decode should return Byte when simple querying postgresql char`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return Byte when extended querying postgresql char`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    companion object {
        private val bytes = byteArrayOf(9, 44, -5)

        @JvmStatic
        @BeforeAll
        fun setup(): Unit = runBlocking {
            PgConnectionHelper.defaultConnection().use { connection ->
                connection.sendQuery("""
                    DROP TABLE IF EXISTS public.char_test;
                    CREATE TABLE public.char_test(char_field "char" not null);
                    INSERT INTO public.char_test(char_field)
                    VALUES${bytes.joinToString(separator = ",") { "(CAST($it as \"char\"))" }};
                """.trimIndent()).release()
            }
        }
    }
}
