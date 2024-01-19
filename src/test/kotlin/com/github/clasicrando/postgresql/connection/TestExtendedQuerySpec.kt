package com.github.clasicrando.postgresql.connection

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.common.result.getInt
import com.github.clasicrando.common.result.getString
import com.github.clasicrando.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

@EnabledIfEnvironmentVariable(named = "PG_TEST_PASSWORD", matches = ".+")
class TestExtendedQuerySpec {
    @BeforeTest
    fun setup(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            it.sendQuery(TEST_PROC)
        }
    }

    @Test
    fun `sendPreparedStatement should return 1 result when regular query`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            val result = it.sendPreparedStatement(QUERY_SERIES, listOf(1, 10)).toList()
            assertEquals(1, result.size)
            val queryResult = result[0]
            assertEquals(10, queryResult.rowsAffected)
            var rowCount = 0
            for ((i, row) in queryResult.rows.withIndex()) {
                rowCount++
                assertEquals(i + 1, row.getInt(0))
                assertEquals("Regular Query", row.getString(1))
            }
            assertEquals(10, rowCount)
        }
    }

    @Test
    fun `sendPreparedStatement should return 1 result when stored procedure with out parameter`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            val param1 = 2
            val param2 = "start"
            val params = listOf<Any?>(param1, param2)
            val result = it.sendPreparedStatement(
                "CALL public.test_proc_ext($1::int, $2::text)",
                params,
            ).toList()
            assertEquals(1, result.size)
            val queryResult = result[0]
            assertEquals(0, queryResult.rowsAffected)
            val rows = queryResult.rows.toList()
            assertEquals(1, rows.size)
            assertEquals(param1 + 1, rows[0].getInt(0))
            assertEquals("$param2,${param1 + 1}", rows[0].getString(1))
        }
    }

    companion object {
        const val TEST_PROC = """
            DROP PROCEDURE IF EXISTS public.test_proc_ext;
            CREATE PROCEDURE public.test_proc_ext(in out int, in out text)
            LANGUAGE plpgsql
            AS $$
            BEGIN
                $1 := COALESCE($1,0) + 1;
                $2 := LTRIM($2 || ',' || $1, ',');
            END;
            $$;
        """

        const val QUERY_SERIES = """
            SELECT s.s, 'Regular Query' t
            FROM generate_series($1::int, $2::int) s
        """
    }
}
