package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.query.fetchAll
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.result.DataRow
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

        PgConnectionHelper.defaultSuspendingConnection().use { conn ->
            val char = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<Byte>()
            assertEquals(value, char)
        }
    }

    object CharTestRowParser : RowParser<Byte> {
        override fun fromRow(row: DataRow): Byte = row.getByte(0)!!
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT char_field FROM char_test ORDER BY char_field;"

        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use { conn ->
            val chars = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }
                .fetchAll(CharTestRowParser)
                .toByteArray()
            Assertions.assertArrayEquals(bytes, chars)
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
            PgConnectionHelper.defaultSuspendingConnection().use { connection ->
                connection.sendSimpleQuery("""
                    DROP TABLE IF EXISTS public.char_test;
                    CREATE TABLE public.char_test(char_field "char" not null);
                    INSERT INTO public.char_test(char_field)
                    VALUES${bytes.joinToString(separator = ",") { "(CAST($it as \"char\"))" }};
                """.trimIndent()).release()
            }
        }
    }
}
