package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.common.result.getByte
import com.github.clasicrando.postgresql.PgConnectionHelper
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

        val result = PgConnectionHelper.defaultConnection().use {
            it.sendPreparedStatement(query, listOf(value))
        }.toList()

        assertEquals(1, result.size)
        assertEquals(1, result[0].rowsAffected)
        val rows = result[0].rows.toList()
        assertEquals(1, rows.size)
        assertEquals(value, rows.map { it.getByte("char_col") }.first())
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT char_field FROM char_test ORDER BY char_field;"

        val result = PgConnectionHelper.defaultConnectionWithForcedSimple().use {
            if (isPrepared) {
                it.sendPreparedStatement(query, emptyList())
            } else {
                it.sendQuery(query)
            }
        }.toList()

        assertEquals(1, result.size)
        assertEquals(3, result[0].rowsAffected)
        val rows = result[0].rows.toList()
        assertEquals(3, rows.size)
        Assertions.assertArrayEquals(bytes, rows.map { it.getByte(0)!! }.toByteArray())
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
