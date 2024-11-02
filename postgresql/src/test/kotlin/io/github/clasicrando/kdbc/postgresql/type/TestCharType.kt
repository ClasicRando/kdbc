package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.fetchAll
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class TestCharType {
    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @ValueSource(bytes = [0, 2, 58, 122])
    fun `encode should accept Byte when querying postgresql`(value: Byte): Unit = runBlocking {
        val query = "SELECT $1 char_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val char = query(query).bind(value).fetchScalar<Byte>(conn)
            assertEquals(value, char)
        }
    }

    object CharTestRowParser : RowParser<Byte> {
        override fun fromRow(row: DataRow): Byte = row.getAsNonNull(0)
    }

    private suspend fun decodeTest(isExtended: Boolean) {
        val query = "SELECT char_field FROM char_test ORDER BY char_field;"
        if (isExtended) {
                PgConnectionHelper.defaultConnection()
            } else {
                PgConnectionHelper.defaultConnectionWithForcedSimple()
            }
            .use { conn ->
                val chars = query(query).fetchAll(conn, CharTestRowParser).toByteArray()
                Assertions.assertArrayEquals(bytes, chars)
            }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return Byte when simple querying postgresql char`(): Unit = runBlocking {
        decodeTest(isExtended = false)
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return Byte when extended querying postgresql char`(): Unit = runBlocking {
        decodeTest(isExtended = true)
    }

    companion object {
        private val bytes = byteArrayOf(9, 44, -5)

        @JvmStatic
        @BeforeAll
        fun setup(): Unit = runBlocking {
            PgConnectionHelper.defaultConnection().use { connection ->
                query(
                        """
                        DROP TABLE IF EXISTS public.char_test;
                        CREATE TABLE public.char_test(char_field "char" not null);
                        INSERT INTO public.char_test(char_field)
                        VALUES${
                            bytes.joinToString(
                                separator = ","
                            ) { "(CAST($it as \"char\"))" }};
                        """
                            .trimIndent()
                    )
                    .execute(connection)
            }
        }
    }
}
