package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.connection.useCatching
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.postgresql.GeneralPostgresError
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TestPipelineQuerySpec {
    @Test
    fun `pipelineQueries should return multiple results with auto commit`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val results = connection.pipelineQueriesSyncAll(
                "SELECT $1 i" to listOf(1),
                "SELECT $1 t" to listOf("Pipeline Query"),
            ).toList()
            assertEquals(2, results.size)
            assertEquals(1, results[0].rowsAffected)
            assertEquals(1, results[0].rows.first().getInt(0))
            assertEquals(1, results[1].rowsAffected)
            assertEquals("Pipeline Query", results[1].rows.first().getString(0))
        }
    }

    @Test
    fun `pipelineQueries should throw exception and keep previous changes when erroneous query and autocommit`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            val results = it.sendSimpleQuery(ROLLBACK_CHECK).toList()
            assertEquals(2, results.size)
            assertEquals(0, results[0].rowsAffected)
            assertEquals(0, results[1].rowsAffected)
        }
        val result = PgConnectionHelper.defaultConnection().useCatching {
            it.pipelineQueriesSyncAll(
                "INSERT INTO public.rollback_check VALUES($1,$2)" to listOf(1, "Pipeline Query"),
                "SELECT $1::int t" to listOf("not int"),
            ).toList()
        }
        assertTrue(result.isFailure)
        val exception = result.exceptionOrNull()
        assertTrue(
            exception is GeneralPostgresError,
            "Exception should be GeneralPostgresError but got ${exception?.toString()}"
        )
        PgConnectionHelper.defaultConnection().use {
            val count = it.createQuery("SELECT COUNT(*) FROM public.rollback_check")
                .fetchScalar<Long>()
            assertEquals(1, count)
        }
    }

    @Test
    fun `pipelineQueries should throw exception and keep previous changes when erroneous query with more queries after and autocommit`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            val results = it.sendSimpleQuery(ROLLBACK_CHECK).toList()
            assertEquals(2, results.size)
            assertEquals(0, results[0].rowsAffected)
            assertEquals(0, results[1].rowsAffected)
        }
        val result = PgConnectionHelper.defaultConnection().useCatching {
            it.pipelineQueriesSyncAll(
                "INSERT INTO public.rollback_check VALUES($1,$2)" to listOf(1, "Pipeline Query"),
                "SELECT $1::int t" to listOf("not int"),
                "SELECT $1 t" to listOf("not int"),
            ).toList()
        }
        assertTrue(result.isFailure)
        val exception = result.exceptionOrNull()
        assertTrue(
            exception is GeneralPostgresError,
            "Exception should be GeneralPostgresError but got ${exception?.toString()}\n${exception?.message}\n${exception?.stackTraceToString()}"
        )
        PgConnectionHelper.defaultConnection().use {
            val count = it.createQuery("SELECT COUNT(*) FROM public.rollback_check")
                .fetchScalar<Long>()
            assertEquals(1, count)
        }
    }

    @Test
    fun `pipelineQueries should throw exception and rollback transaction when erroneous query and not auto commit`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            val results = it.sendSimpleQuery(ROLLBACK_CHECK).toList()
            assertEquals(2, results.size)
            assertEquals(0, results[0].rowsAffected)
            assertEquals(0, results[1].rowsAffected)
        }
        val result = PgConnectionHelper.defaultConnection().useCatching {
            it.pipelineQueries(
                syncAll = false,
                queries = arrayOf(
                    "INSERT INTO public.rollback_check VALUES($1,$2)" to listOf(1, "Pipeline Query"),
                    "SELECT $1::int t" to listOf("not int"),
                ),
            ).toList()
        }
        assertTrue(result.isFailure)
        val exception = result.exceptionOrNull()
        assertTrue(
            exception is GeneralPostgresError,
            "Exception should be GeneralPostgresError but got ${exception?.toString()}"
        )
        PgConnectionHelper.defaultConnection().use {
            val count = it.createQuery("SELECT COUNT(*) FROM public.rollback_check")
                .fetchScalar<Long>()
            assertEquals(0, count)
        }
    }

    companion object {
        private const val ROLLBACK_CHECK = """
            DROP TABLE IF EXISTS public.rollback_check;
            CREATE TABLE public.rollback_check(id int, value text);
        """
    }
}
