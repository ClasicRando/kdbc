package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.core.useCatching
import io.github.clasicrando.kdbc.postgresql.GeneralPostgresError
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking

class TestPipelineQuerySpec {
    @Test
    fun `pipelineQueries should return multiple results with auto commit`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val (results, rowSets) =
                connection
                    .pipelineQueries(
                        syncAll = true,
                        queries =
                        listOf(
                            query("SELECT $1 i").bind(1),
                            query("SELECT $1 t").bind("Pipeline Query"),
                        ),
                    )
                    .collectResults()
            assertEquals(2, results.size)
            assertEquals(2, rowSets.size)
            assertEquals(1, results[0].rowsAffected)
            assertEquals(1, rowSets[0].first().getAsNonNull(0))
            assertEquals(1, results[1].rowsAffected)
            assertEquals("Pipeline Query", rowSets[1].first().getAsNonNull(0))
        }
    }

    @Test
    fun `pipelineQueries should throw exception and keep previous changes when erroneous query and autocommit`():
        Unit = runBlocking {
        val result =
            PgConnectionHelper.defaultConnection().useCatching {
                it.pipelineQueries(
                    syncAll = true,
                    queries =
                    listOf(
                        query("INSERT INTO public.rollback_check VALUES($1,$2)")
                            .bind(1)
                            .bind("Pipeline Query"),
                        query("SELECT $1::int t").bind("not int"),
                    ),
                )
                    .toList()
            }
        assertTrue(result.isFailure)
        val exception = result.exceptionOrNull()
        assertTrue(
            exception is GeneralPostgresError,
            "Exception should be GeneralPostgresError but got ${exception?.toString()}",
        )
        PgConnectionHelper.defaultConnection().use {
            val count = query("SELECT COUNT(*) FROM public.rollback_check").fetchScalar<Long>(it)
            assertEquals(1, count)
        }
    }

    @Test
    fun `pipelineQueries should throw exception and keep previous changes when erroneous query with more queries after and autocommit`():
        Unit = runBlocking {
        val result =
            PgConnectionHelper.defaultConnection().useCatching {
                it.pipelineQueries(
                    syncAll = true,
                    queries =
                    listOf(
                        query("INSERT INTO public.rollback_check VALUES($1,$2)")
                            .bind(1)
                            .bind("Pipeline Query"),
                        query("SELECT $1::int t").bind("not int"),
                        query("SELECT $1 t").bind("not int"),
                    ),
                )
                    .collect()
            }
        assertTrue(result.isFailure)
        val exception = result.exceptionOrNull()
        assertTrue(
            exception is GeneralPostgresError,
            "Exception should be GeneralPostgresError but got ${exception?.toString()}\n${exception?.message}\n${exception?.stackTraceToString()}",
        )
        PgConnectionHelper.defaultConnection().use {
            val count = query("SELECT COUNT(*) FROM public.rollback_check").fetchScalar<Long>(it)
            assertEquals(1, count)
        }
    }

    @Test
    fun `pipelineQueries should throw exception and rollback transaction when erroneous query and not auto commit`():
        Unit = runBlocking {
        val result =
            PgConnectionHelper.defaultConnection().useCatching {
                it.pipelineQueries(
                    syncAll = false,
                    queries =
                    listOf(
                        query("INSERT INTO public.rollback_check VALUES($1,$2)")
                            .bind(1)
                            .bind("Pipeline Query"),
                        query("SELECT $1::int t").bind("not int"),
                    ),
                )
                    .collect()
            }
        assertTrue(result.isFailure)
        val exception = result.exceptionOrNull()
        assertTrue(
            exception is GeneralPostgresError,
            "Exception should be GeneralPostgresError but got ${exception?.toString()}",
        )
        PgConnectionHelper.defaultConnection().use {
            val count = query("SELECT COUNT(*) FROM public.rollback_check").fetchScalar<Long>(it)
            assertEquals(0, count)
        }
    }

    @BeforeTest
    fun init(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { query(ROLLBACK_CHECK).execute(it) }
    }

    companion object {
        private const val ROLLBACK_CHECK =
            """
            DROP TABLE IF EXISTS public.rollback_check;
            CREATE TABLE public.rollback_check(id int, value text);
        """
    }
}
