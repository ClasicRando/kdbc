package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlin.reflect.typeOf
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

class TestExtendedQuerySpec {
    @BeforeTest
    fun setup(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { query(TEST_PROC).execute(it) }
    }

    @Test
    fun `sendExtendedQuery should return 1 result when regular query`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            val (results, rowSets) =
                it.sendExtendedQuery(
                        QUERY_SERIES,
                        listOf(QueryParameter(1, typeOf<Int>()), QueryParameter(10, typeOf<Int>())),
                    )
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
    fun `sendExtendedQuery should return 1 result when stored procedure with out parameter`():
        Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            val param1 = 2
            val param2 = "start"
            val params =
                listOf(
                    QueryParameter(param1, typeOf<Int>()),
                    QueryParameter(param2, typeOf<String>()),
                )
            val (results, rowSets) =
                it.sendExtendedQuery("CALL public.test_proc_ext($1::int, $2::text)", params)
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

    companion object {
        const val TEST_PROC =
            """
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

        const val QUERY_SERIES =
            """
            SELECT s.s, 'Regular Query' t
            FROM generate_series($1::int, $2::int) s
        """
    }
}
