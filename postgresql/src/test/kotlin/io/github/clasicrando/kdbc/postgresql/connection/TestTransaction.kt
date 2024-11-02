package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.connection.transactionCatching
import io.github.clasicrando.kdbc.core.pool.useConnection
import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll

class TestTransaction {
    @BeforeTest
    fun cleanUp(): Unit = runBlocking {
        pool.useConnection { query("TRUNCATE TABLE public.$TABLE_NAME").execute(it) }
    }

    @Test
    fun `transaction commits when successful SQL statement`(): Unit = runBlocking {
        pool.useConnection { conn ->
            val countBefore =
                query("SELECT COUNT(0) FROM public.$TABLE_NAME").fetchScalar<Long>(conn)
            assertEquals(0L, countBefore)
            val result =
                conn.transactionCatching {
                    query("INSERT INTO public.$TABLE_NAME VALUES(1, '')").execute(it)
                    query("INSERT INTO public.$TABLE_NAME VALUES(2, '')").execute(it)
                }
            assertTrue(result.isSuccess)
            val count = query("SELECT COUNT(0) FROM public.$TABLE_NAME").fetchScalar<Long>(conn)
            assertEquals(2L, count)
        }
    }

    @Test
    fun `transaction rolls back when failed SQL statement`(): Unit = runBlocking {
        pool.useConnection { conn ->
            val countBefore =
                query("SELECT COUNT(0) FROM public.$TABLE_NAME").fetchScalar<Long>(conn)
            assertEquals(0L, countBefore)
            val result =
                conn.transactionCatching {
                    query("INSERT INTO public.$TABLE_NAME VALUES(1, '')").execute(it)
                    query("INSERT INTO public.$TABLE_NAME VALUES(2, null)").execute(it)
                }
            assertTrue(result.isFailure)
            val countAfter =
                query("SELECT COUNT(0) FROM public.$TABLE_NAME").fetchScalar<Long>(conn)
            assertEquals(0L, countAfter)
        }
    }

    companion object {
        private const val TABLE_NAME = "transaction_test"
        private const val CREATE_TABLE =
            """
            DROP TABLE IF EXISTS public.$TABLE_NAME;
            CREATE TABLE public.$TABLE_NAME(id int not null, text_field text not null);
        """
        private val pool = PgConnectionHelper.defaultPool()

        @JvmStatic
        @BeforeAll
        fun setup(): Unit = runBlocking { pool.useConnection { it.sendSimpleQuery(CREATE_TABLE) } }

        @JvmStatic @AfterAll fun tearDown(): Unit = runBlocking { pool.close() }
    }
}
