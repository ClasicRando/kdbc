package com.github.kdbc.postgresql.connection

import com.github.kdbc.core.connection.use
import com.github.kdbc.core.result.getInt
import com.github.kdbc.postgresql.GeneralPostgresError
import com.github.kdbc.postgresql.PgConnectionHelper
import com.github.kdbc.postgresql.message.information.Severity
import com.github.kdbc.postgresql.message.information.SqlState
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test
import kotlin.test.assertEquals

class TestSimpleQuerySpec {

    @Test
    fun `sendQuery should return 1 result when regular query`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnectionWithForcedSimple().use {
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
        PgConnectionHelper.defaultConnectionWithForcedSimple().use {
            val result = it.sendQuery("CALL public.$TEST_PROC_NAME(null, null)").toList()
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
        PgConnectionHelper.defaultConnectionWithForcedSimple().use { connection ->
            val queries = """
                CALL public.$TEST_PROC_NAME(null, null);
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

    @Test
    fun `sendQuery should timeout when long running query with timeout specified`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnectionWithQueryTimeout().use { connection ->
            val queries = "CALL public.$LONG_RUNNING_TEST_PROC_NAME()"
            val exception = assertThrows<GeneralPostgresError> { connection.sendQuery(queries) }
            assertEquals(Severity.ERROR, exception.errorInformation.severity)
            assertEquals(SqlState.QueryCanceled, exception.errorInformation.code)
        }
    }

    companion object {
        const val TEST_PROC_NAME = "test_proc"
        const val LONG_RUNNING_TEST_PROC_NAME = "long_running_test_proc"
        private const val STARTUP_SCRIPT = """
            DROP PROCEDURE IF EXISTS public.$TEST_PROC_NAME;
            CREATE PROCEDURE public.$TEST_PROC_NAME(out int, out text)
            LANGUAGE plpgsql
            AS $$
            BEGIN
                $1 := 4;
                $2 := 'This is a test';
            END;
            $$;
            DROP PROCEDURE IF EXISTS public.$LONG_RUNNING_TEST_PROC_NAME;
            CREATE PROCEDURE public.$LONG_RUNNING_TEST_PROC_NAME()
            LANGUAGE sql
            AS $$
            SELECT pg_sleep(5);
            $$;
        """

        const val QUERY_SERIES = """
            SELECT s.s, 'Regular Query' t
            FROM generate_series(1, 10) s
        """

        @JvmStatic
        @BeforeAll
        fun setup(): Unit = runBlocking {
            PgConnectionHelper.defaultConnection().use {
                it.sendQuery(STARTUP_SCRIPT)
            }
        }
    }
}
