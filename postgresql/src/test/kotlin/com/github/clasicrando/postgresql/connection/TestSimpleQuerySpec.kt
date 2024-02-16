package com.github.clasicrando.postgresql.connection

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.common.result.getInt
import com.github.clasicrando.common.result.getString
import com.github.clasicrando.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

class TestSimpleQuerySpec {
    @BeforeTest
    fun setup(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            it.sendQuery(TEST_PROC)
        }
    }

    @Test
    fun `sendQuery should return 1 result when regular query`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            val result = it.sendQuery(QUERY_SERIES).toList()
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
    fun `sendQuery should return 1 result when stored procedure with out parameter`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            val result = it.sendQuery("CALL public.test_proc(null, null)").toList()
            assertEquals(1, result.size)
            val queryResult = result[0]
            assertEquals(0, queryResult.rowsAffected)
            val rows = queryResult.rows.toList()
            assertEquals(1, rows.size)
            assertEquals(4, rows[0].getInt(0))
            assertEquals("This is a test", rows[0].getString(1))
        }
    }

    @Test
    fun `sendQuery should return multiple results when multiple statements`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val queries = """
                CALL public.test_proc(null, null);
                SELECT 1 test_i;
            """.trimIndent()
            val results = connection.sendQuery(queries).toList()
            assertEquals(2, results.size)
            assertEquals(0, results[0].rowsAffected)
            assertEquals(4, results[0].rows.firstOrNull()?.getInt(0))
            assertEquals(1, results[1].rowsAffected)
            assertEquals(1, results[1].rows.firstOrNull()?.getInt("test_i"))
        }
    }

    companion object {
        const val TEST_PROC = """
            DROP PROCEDURE IF EXISTS public.test_proc;
            CREATE PROCEDURE public.test_proc(out int, out text)
            LANGUAGE plpgsql
            AS $$
            BEGIN
                $1 := 4;
                $2 := 'This is a test';
            END;
            $$;
        """

        const val QUERY_SERIES = """
            SELECT s.s, 'Regular Query' t
            FROM generate_series(1, 10) s
        """
    }
}
