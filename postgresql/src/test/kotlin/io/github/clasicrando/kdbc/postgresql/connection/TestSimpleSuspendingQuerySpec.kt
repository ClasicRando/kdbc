package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.result.getAs
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.postgresql.GeneralPostgresError
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.message.information.Severity
import io.github.clasicrando.kdbc.postgresql.message.information.SqlState
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test
import kotlin.test.assertEquals

class TestSimpleSuspendingQuerySpec {

    @Test
    fun `sendSimpleQuery should return 1 result when regular query`(): Unit = runBlocking {
        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use {
            val result = it.sendSimpleQuery(QUERY_SERIES).toList()
            assertEquals(1, result.size)
            val queryResult = result[0]
            assertEquals(10, queryResult.rowsAffected)
            var rowCount = 0
            for ((i, row) in queryResult.rows.withIndex()) {
                rowCount++
                assertEquals(i + 1, row.getAsNonNull(0))
                assertEquals("Regular Query", row.getAsNonNull(1))
            }
            assertEquals(10, rowCount)
        }
    }

    @Test
    fun `sendSimpleQuery should return 1 result when stored procedure with out parameter`(): Unit = runBlocking {
        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use {
            val result = it.sendSimpleQuery("CALL public.$TEST_PROC_NAME(null, null)").toList()
            assertEquals(1, result.size)
            val queryResult = result[0]
            assertEquals(0, queryResult.rowsAffected)
            val rows = queryResult.rows.toList()
            assertEquals(1, rows.size)
            assertEquals(4, rows[0].getAsNonNull(0))
            assertEquals("This is a test", rows[0].getAsNonNull(1))
        }
    }

    @Test
    fun `sendSimpleQuery should return multiple results when multiple statements`(): Unit = runBlocking {
        PgConnectionHelper.defaultSuspendingConnectionWithForcedSimple().use { connection ->
            val queries = """
                CALL public.$TEST_PROC_NAME(null, null);
                SELECT 1 test_i;
            """.trimIndent()
            val results = connection.sendSimpleQuery(queries).toList()
            assertEquals(2, results.size)
            assertEquals(0, results[0].rowsAffected)
            assertEquals(4, results[0].rows.firstOrNull()?.getAs(0))
            assertEquals(1, results[1].rowsAffected)
            assertEquals(1, results[1].rows.firstOrNull()?.getAs("test_i"))
        }
    }

    @Test
    fun `sendSimpleQuery should timeout when long running query with timeout specified`(): Unit = runBlocking {
        PgConnectionHelper.defaultSuspendingConnectionWithQueryTimeout().use { connection ->
            val queries = "CALL public.$LONG_RUNNING_TEST_PROC_NAME()"
            val exception = assertThrows<GeneralPostgresError> { connection.sendSimpleQuery(queries) }
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
            PgConnectionHelper.defaultSuspendingConnection().use {
                it.sendSimpleQuery(STARTUP_SCRIPT)
            }
        }
    }
}
