package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlin.reflect.KType
import kotlin.reflect.typeOf
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals

class TestBlockingExtendedSuspendingQuerySpec {
    @BeforeTest
    fun setup() {
        PgConnectionHelper.defaultBlockingConnection().use {
            it.sendSimpleQuery(TEST_PROC)
        }
    }

    @Test
    fun `sendPreparedStatement should return 1 result when regular query`() {
        PgConnectionHelper.defaultBlockingConnection().use {
            val result = it.sendExtendedQuery(
                QUERY_SERIES,
                listOf(
                    1 to typeOf<Int>(),
                    10 to typeOf<Int>(),
                )
            ).toList()
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
    fun `sendPreparedStatement should return 1 result when stored procedure with out parameter`() {
        PgConnectionHelper.defaultBlockingConnection().use {
            val param1 = 2
            val param2 = "start"
            val params = listOf<Pair<Any?, KType>>(
                param1 to typeOf<Int>(),
                param2 to typeOf<String>(),
            )
            val result = it.sendExtendedQuery(
                "CALL public.test_proc_ext($1::int, $2::text)",
                params,
            ).toList()
            assertEquals(1, result.size)
            val queryResult = result[0]
            assertEquals(0, queryResult.rowsAffected)
            val rows = queryResult.rows.toList()
            assertEquals(1, rows.size)
            assertEquals(param1 + 1, rows[0].getAsNonNull(0))
            assertEquals("$param2,${param1 + 1}", rows[0].getAsNonNull(1))
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
