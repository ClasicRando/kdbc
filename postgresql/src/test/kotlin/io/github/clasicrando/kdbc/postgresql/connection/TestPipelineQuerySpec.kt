package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.query.QueryParameter
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.core.useCatching
import io.github.clasicrando.kdbc.postgresql.GeneralPostgresError
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlin.reflect.typeOf
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking

class TestPipelineQuerySpec {
    @Test
    fun `pipelineQueries should return multiple results with auto commit`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val results =
                connection
                    .pipelineQueries(
                        syncAll = true,
                        "SELECT $1 i" to listOf(QueryParameter(1, typeOf<Int>())),
                        "SELECT $1 t" to listOf(QueryParameter("Pipeline Query", typeOf<String>())),
                    )
                    .toList()
            assertEquals(2, results.size)
            assertEquals(1, results[0].rowsAffected)
            assertEquals(1, results[0].rows.first().getAsNonNull(0))
            assertEquals(1, results[1].rowsAffected)
            assertEquals("Pipeline Query", results[1].rows.first().getAsNonNull(0))
        }
    }

    @Test
    fun `pipelineQueries should throw exception and keep previous changes when erroneous query and autocommit`():
        Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use {
            val results = it.sendSimpleQuery(ROLLBACK_CHECK).toList()
            assertEquals(2, results.size)
            assertEquals(0, results[0].rowsAffected)
            assertEquals(0, results[1].rowsAffected)
        }
        val result =
            PgConnectionHelper.defaultConnection().useCatching {
                it.pipelineQueries(
                        syncAll = true,
                        "INSERT INTO public.rollback_check VALUES($1,$2)" to
                            listOf(
                                QueryParameter(1, typeOf<Int>()),
                                QueryParameter("Pipeline Query", typeOf<String>()),
                            ),
                        "SELECT $1::int t" to listOf(QueryParameter("not int", typeOf<String>())),
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
        PgConnectionHelper.defaultConnection().use {
            val results = it.sendSimpleQuery(ROLLBACK_CHECK).toList()
            assertEquals(2, results.size)
            assertEquals(0, results[0].rowsAffected)
            assertEquals(0, results[1].rowsAffected)
        }
        val result =
            PgConnectionHelper.defaultConnection().useCatching {
                it.pipelineQueries(
                        syncAll = true,
                        "INSERT INTO public.rollback_check VALUES($1,$2)" to
                            listOf(
                                QueryParameter(1, typeOf<Int>()),
                                QueryParameter("Pipeline Query", typeOf<String>()),
                            ),
                        "SELECT $1::int t" to listOf(QueryParameter("not int", typeOf<String>())),
                        "SELECT $1 t" to listOf(QueryParameter("not int", typeOf<String>())),
                    )
                    .toList()
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
        PgConnectionHelper.defaultConnection().use {
            val results = it.sendSimpleQuery(ROLLBACK_CHECK).toList()
            assertEquals(2, results.size)
            assertEquals(0, results[0].rowsAffected)
            assertEquals(0, results[1].rowsAffected)
        }
        val result =
            PgConnectionHelper.defaultConnection().useCatching {
                it.pipelineQueries(
                        syncAll = false,
                        queries =
                            arrayOf(
                                "INSERT INTO public.rollback_check VALUES($1,$2)" to
                                    listOf(
                                        QueryParameter(1, typeOf<Int>()),
                                        QueryParameter("Pipeline Query", typeOf<String>()),
                                    ),
                                "SELECT $1::int t" to
                                    listOf(QueryParameter("not int", typeOf<String>())),
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
            assertEquals(0, count)
        }
    }

    companion object {
        private const val ROLLBACK_CHECK =
            """
            DROP TABLE IF EXISTS public.rollback_check;
            CREATE TABLE public.rollback_check(id int, value text);
        """
    }
}
