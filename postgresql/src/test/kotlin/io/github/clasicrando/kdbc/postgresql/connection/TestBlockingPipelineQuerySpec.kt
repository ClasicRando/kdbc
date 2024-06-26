package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.connection.useCatching
import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.postgresql.GeneralPostgresError
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TestBlockingPipelineQuerySpec {
    @Test
    fun `pipelineQueries should return multiple results with auto commit`() {
        PgConnectionHelper.defaultBlockingConnection().use { connection ->
            val results = connection.pipelineQueriesSyncAll(
                "SELECT $1 i" to listOf(QueryParameter(1)),
                "SELECT $1 t" to listOf(QueryParameter("Pipeline Query")),
            ).toList()
            assertEquals(2, results.size)
            assertEquals(1, results[0].rowsAffected)
            assertEquals(1, results[0].rows.first().getAsNonNull(0))
            assertEquals(1, results[1].rowsAffected)
            assertEquals("Pipeline Query", results[1].rows.first().getAsNonNull(0))
        }
    }

    @Test
    fun `pipelineQueries should throw exception and keep previous changes when erroneous query and autocommit`() {
        PgConnectionHelper.defaultBlockingConnection().use {
            val results = it.sendSimpleQuery(ROLLBACK_CHECK).toList()
            assertEquals(2, results.size)
            assertEquals(0, results[0].rowsAffected)
            assertEquals(0, results[1].rowsAffected)
        }
        val result = PgConnectionHelper.defaultBlockingConnection().useCatching {
            it.pipelineQueriesSyncAll(
                "INSERT INTO public.rollback_check VALUES($1,$2)" to listOf(
                    QueryParameter(1),
                    QueryParameter("Pipeline Query"),
                ),
                "SELECT $1::int t" to listOf(QueryParameter("not int")),
            ).toList()
        }
        assertTrue(result.isFailure)
        val exception = result.exceptionOrNull()
        assertTrue(
            exception is GeneralPostgresError,
            "Exception should be GeneralPostgresError but got ${exception?.toString()}"
        )
        PgConnectionHelper.defaultBlockingConnection().use {
            val results = it.sendSimpleQuery("SELECT COUNT(*) FROM public.rollback_check").toList()
            assertEquals(1, results.size)
            assertEquals(1, results[0].rowsAffected)
            assertEquals(1L, results[0].rows.first().getAsNonNull(0))
        }
    }

    @Test
    fun `pipelineQueries should throw exception and keep previous changes when erroneous query with more queries after and autocommit`() {
        PgConnectionHelper.defaultBlockingConnection().use {
            val results = it.sendSimpleQuery(ROLLBACK_CHECK).toList()
            assertEquals(2, results.size)
            assertEquals(0, results[0].rowsAffected)
            assertEquals(0, results[1].rowsAffected)
        }
        val result = PgConnectionHelper.defaultBlockingConnection().useCatching {
            it.pipelineQueriesSyncAll(
                "INSERT INTO public.rollback_check VALUES($1,$2)" to listOf(
                    QueryParameter(1),
                    QueryParameter("Pipeline Query"),
                ),
                "SELECT $1::int t" to listOf(QueryParameter("not int")),
                "SELECT $1 t" to listOf(QueryParameter("not int")),
            ).toList()
        }
        assertTrue(result.isFailure)
        val exception = result.exceptionOrNull()
        assertTrue(
            exception is GeneralPostgresError,
            "Exception should be GeneralPostgresError but got ${exception?.toString()}\n${exception?.message}\n${exception?.stackTraceToString()}"
        )
        PgConnectionHelper.defaultBlockingConnection().use {
            val results = it.sendSimpleQuery("SELECT COUNT(*) FROM public.rollback_check").toList()
            assertEquals(1, results.size)
            assertEquals(1, results[0].rowsAffected)
            assertEquals(1L, results[0].rows.first().getAsNonNull(0))
        }
    }

    @Test
    fun `pipelineQueries should throw exception and rollback transaction when erroneous query and not auto commit`() {
        PgConnectionHelper.defaultBlockingConnection().use {
            val results = it.sendSimpleQuery(ROLLBACK_CHECK).toList()
            assertEquals(2, results.size)
            assertEquals(0, results[0].rowsAffected)
            assertEquals(0, results[1].rowsAffected)
        }
        val result = PgConnectionHelper.defaultBlockingConnection().useCatching {
            it.pipelineQueries(
                syncAll = false,
                queries = arrayOf(
                    "INSERT INTO public.rollback_check VALUES($1,$2)" to listOf(
                        QueryParameter(1),
                        QueryParameter("Pipeline Query"),
                    ),
                    "SELECT $1::int t" to listOf(QueryParameter("not int")),
                ),
            ).toList()
        }
        assertTrue(result.isFailure)
        val exception = result.exceptionOrNull()
        assertTrue(
            exception is GeneralPostgresError,
            "Exception should be GeneralPostgresError but got ${exception?.toString()}"
        )
        PgConnectionHelper.defaultBlockingConnection().use {
            val results = it.sendSimpleQuery("SELECT COUNT(*) FROM public.rollback_check").toList()
            assertEquals(1, results.size)
            assertEquals(1, results[0].rowsAffected)
            assertEquals(0L, results[0].rows.first().getAsNonNull(0))
        }
    }

    companion object {
        private const val ROLLBACK_CHECK = """
            DROP TABLE IF EXISTS public.rollback_check;
            CREATE TABLE public.rollback_check(id int, value text);
        """
    }
}
