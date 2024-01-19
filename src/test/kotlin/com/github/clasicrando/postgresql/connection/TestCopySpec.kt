package com.github.clasicrando.postgresql.connection

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.common.connection.useCatching
import com.github.clasicrando.common.result.getLong
import com.github.clasicrando.postgresql.GeneralPostgresError
import com.github.clasicrando.postgresql.PgConnectionHelper
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@EnabledIfEnvironmentVariable(named = "PG_TEST_PASSWORD", matches = ".+")
class TestCopySpec {
    @BeforeTest
    fun setup(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            it.sendQuery(CREATE_COPY_TARGET_TABLE)
            it.sendQuery(CREATE_COPY_FROM_TABLE)
        }
    }

    @Test
    fun `copyIn should copy all rows`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            it.sendQuery("TRUNCATE public.copy_in_test;")
            val copyResult = it.copyIn(
                "COPY public.copy_in_test FROM STDIN WITH (FORMAT csv)",
                (1..ROW_COUNT).map { i -> "$i,$i Value\n".toByteArray() }.asFlow(),
            )
            assertEquals(ROW_COUNT, copyResult.rowsAffected)
            assertEquals("COPY $ROW_COUNT", copyResult.message)
            val results = it.sendQuery("SELECT COUNT(*) FROM public.copy_in_test").toList()
            assertEquals(1, results.size)
            assertEquals(1, results[0].rowsAffected)
            assertEquals(ROW_COUNT, results[0].rows.first().getLong(0))
        }
    }

    @Test
    fun `copyIn should throw exception when improperly formatted rows`(): Unit = runBlocking {
        val result = PgConnectionHelper.defaultConnection().useCatching {
            it.sendQuery("TRUNCATE public.copy_in_test;")
            it.copyIn(
                "COPY public.copy_in_test FROM STDIN WITH (FORMAT csv);",
                (1..ROW_COUNT).map { i -> "$i,$i Value".toByteArray() }.asFlow(),
            )
        }
        assertTrue(result.isFailure)
        assertTrue(result.exceptionOrNull() is GeneralPostgresError)
        PgConnectionHelper.defaultConnection().use {
            val results = it.sendQuery("SELECT COUNT(*) FROM public.copy_in_test;").toList()
            assertEquals(1, results.size)
            assertEquals(1, results[0].rowsAffected)
            assertEquals(0, results[0].rows.first().getLong(0))
        }
    }

    @Test
    fun `copyOut should supply all rows from table`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            val channel = it.copyOut("COPY public.copy_out_test TO STDOUT WITH (FORMAT csv);")
            var rowIndex = 0L
            channel.receiveAsFlow().collect { bytes ->
                rowIndex++
                val str = bytes.toString(Charsets.UTF_8)
                assertEquals("$rowIndex,$rowIndex Value\n", str)
            }
            assertEquals(ROW_COUNT, rowIndex)
        }
    }

    companion object {
        private const val ROW_COUNT = 1_000_000L
        private const val CREATE_COPY_TARGET_TABLE = """
            DROP TABLE IF EXISTS public.copy_in_test;
            CREATE TABLE public.copy_in_test(id int not null, text_field text not null);
        """
        private const val CREATE_COPY_FROM_TABLE = """
            DROP TABLE IF EXISTS public.copy_out_test;
            CREATE TABLE public.copy_out_test(id int not null, text_field text not null);
            INSERT INTO public.copy_out_test(id, text_field)
            SELECT t.t, t.t || ' Value'
            FROM generate_series(1, 1000000) t
        """
    }
}
