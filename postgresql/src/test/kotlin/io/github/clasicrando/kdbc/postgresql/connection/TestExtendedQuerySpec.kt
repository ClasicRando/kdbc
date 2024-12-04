package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlin.reflect.typeOf
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll

class TestExtendedQuerySpec {

    @Test
    fun `sendExtendedQuery should return 1 result when regular query`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            val (results, rowSets) =
                it.sendExtendedQuery(QUERY_SERIES, listOf(QueryParameter(1), QueryParameter(10)))
                    .collectResults()
            assertEquals(1, results.size)
            assertEquals(1, rowSets.size)
            val queryResult = results[0]
            assertEquals(10, queryResult.rowsAffected)
            val rows = rowSets[0]
            var rowCount = 0
            for ((i, row) in rows.withIndex()) {
                rowCount++
                assertEquals(i + 1, row.getAsNonNull(0))
                assertEquals("Regular Query", row.getAsNonNull(1))
            }
            assertEquals(10, rowCount)
        }
    }

    @Test
    fun `sendExtendedQuery should return 1 result when stored procedure with in out parameter`():
        Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            val param1 = 2
            val param2 = "start"
            val params = listOf(QueryParameter(param1), QueryParameter(param2))
            val (results, rowSets) =
                it.sendExtendedQuery("CALL public.$TEST_PROC_IN_OUT($1::int, $2::text)", params)
                    .collectResults()
            assertEquals(1, results.size)
            assertEquals(1, rowSets.size)
            val queryResult = results[0]
            assertEquals(0, queryResult.rowsAffected)
            val rows = rowSets[0]
            assertEquals(1, rows.size)
            assertEquals(param1 + 1, rows[0].getAsNonNull(0))
            assertEquals("$param2,${param1 + 1}", rows[0].getAsNonNull(1))
        }
    }

    @Test
    fun `sendExtendedQuery should return 1 result when stored procedure with out parameter`():
        Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            val params =
                listOf(
                    QueryParameter(value = null, typeOf<Int>()),
                    QueryParameter(value = null, typeOf<String>()),
                )
            val (results, rowSets) =
                it.sendExtendedQuery("CALL public.$TEST_PROC_OUT_NAME($1::int, $2::text)", params)
                    .collectResults()
            assertEquals(1, results.size)
            assertEquals(1, rowSets.size)
            val queryResult = results[0]
            assertEquals(0, queryResult.rowsAffected)
            val rows = rowSets[0]
            assertEquals(1, rows.size)
            assertEquals(1, rows[0].getAsNonNull(0))
            assertEquals("This is a test", rows[0].getAsNonNull(1))
        }
    }

    companion object {
        private const val TEST_PROC_IN_OUT = "test_proc_ext"
        private const val TEST_PROC_OUT_NAME = "test_proc_out"
        const val TEST_PROC =
            """
            DROP PROCEDURE IF EXISTS public.$TEST_PROC_IN_OUT;
            CREATE PROCEDURE public.$TEST_PROC_IN_OUT(in out int, in out text)
            LANGUAGE plpgsql
            AS $$
            BEGIN
                $1 := COALESCE($1,0) + 1;
                $2 := LTRIM($2 || ',' || $1, ',');
            END;
            $$;
        """
        const val TEST_PROC_OUT =
            """
            DROP PROCEDURE IF EXISTS public.$TEST_PROC_OUT_NAME;
            CREATE PROCEDURE public.$TEST_PROC_OUT_NAME(out int, out text)
            LANGUAGE plpgsql
            AS $$
            BEGIN
                $1 := 1;
                $2 := 'This is a test';
            END;
            $$;
            """

        const val QUERY_SERIES =
            """
            SELECT s.s, 'Regular Query' t
            FROM generate_series($1::int, $2::int) s
        """

        @JvmStatic
        @BeforeAll
        fun setup(): Unit = runBlocking {
            PgConnectionHelper.defaultConnection().use {
                query(TEST_PROC).execute(it)
                query(TEST_PROC_OUT).execute(it)
            }
        }
    }
}
