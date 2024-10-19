package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchAll
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.Test
import kotlin.test.assertEquals

class TestCharType {
    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(bytes = [0, 2, 58, 122])
    fun `encode should accept Byte when querying postgresql`(value: Byte): Unit = runBlocking {
        val query = "SELECT $1 char_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val char = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<Byte>()
            assertEquals(value, char)
        }
    }

    object CharTestRowParser : RowParser<Byte> {
        override fun fromRow(row: DataRow): Byte = row.getAsNonNull(0)
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT char_field FROM char_test ORDER BY char_field;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
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
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return Byte when simple querying postgresql char`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return Byte when extended querying postgresql char`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    companion object {
        private val bytes = byteArrayOf(9, 44, -5)

        @JvmStatic
        @BeforeAll
        fun setup(): Unit = runBlocking {
            PgConnectionHelper.defaultConnection().use { connection ->
                connection.sendSimpleQuery("""
                    DROP TABLE IF EXISTS public.char_test;
                    CREATE TABLE public.char_test(char_field "char" not null);
                    INSERT INTO public.char_test(char_field)
                    VALUES${bytes.joinToString(separator = ",") { "(CAST($it as \"char\"))" }};
                """.trimIndent()).close()
            }
        }
    }
}
