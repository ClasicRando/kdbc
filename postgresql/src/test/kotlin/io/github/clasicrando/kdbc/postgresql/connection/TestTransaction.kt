package io.github.clasicrando.kdbc.postgresql.connection

import io.github.clasicrando.kdbc.core.connection.transactionCatching
import io.github.clasicrando.kdbc.core.pool.useConnection
import io.github.clasicrando.kdbc.core.query.executeClosing
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TestTransaction {
    @BeforeTest
    fun cleanUp(): Unit = runBlocking {
        pool.useConnection {
            it.createQuery("TRUNCATE TABLE public.$TABLE_NAME").executeClosing()
        }
    }

    @Test
    fun `transaction commits when successful SQL statement`(): Unit = runBlocking {
        pool.useConnection { conn ->
            val countBefore = conn.createQuery("SELECT COUNT(0) FROM public.$TABLE_NAME")
                .fetchScalar<Long>()
            assertEquals(0L, countBefore)
            val result = conn.transactionCatching {
                it.createQuery("INSERT INTO public.$TABLE_NAME VALUES(1, '')")
                    .executeClosing()
                it.createQuery("INSERT INTO public.$TABLE_NAME VALUES(2, '')")
                    .executeClosing()
            }
            assertTrue(result.isSuccess)
            val count = conn.createQuery("SELECT COUNT(0) FROM public.$TABLE_NAME")
                .fetchScalar<Long>()
            assertEquals(2L, count)
        }
    }

    @Test
    fun `transaction rolls back when failed SQL statement`(): Unit = runBlocking {
        pool.useConnection { conn ->
            val countBefore = conn.createQuery("SELECT COUNT(0) FROM public.$TABLE_NAME")
                .fetchScalar<Long>()
            assertEquals(0L, countBefore)
            val result = conn.transactionCatching {
                it.createQuery("INSERT INTO public.$TABLE_NAME VALUES(1, '')")
                    .executeClosing()
                it.createQuery("INSERT INTO public.$TABLE_NAME VALUES(2, null)")
                    .executeClosing()
            }
            assertTrue(result.isFailure)
            val countAfter = conn.createQuery("SELECT COUNT(0) FROM public.$TABLE_NAME")
                .fetchScalar<Long>()
            assertEquals(0L, countAfter)
        }
    }

    companion object {
        private const val TABLE_NAME = "transaction_test"
        private const val CREATE_TABLE = """
            DROP TABLE IF EXISTS public.$TABLE_NAME;
            CREATE TABLE public.$TABLE_NAME(id int not null, text_field text not null);
        """
        private val pool = PgConnectionHelper.defaultPool()

        @JvmStatic
        @BeforeAll
        fun setup(): Unit = runBlocking {
            pool.useConnection {
                it.sendSimpleQuery(CREATE_TABLE)
            }
        }

        @JvmStatic
        @AfterAll
        fun tearDown(): Unit = runBlocking {
            pool.close()
        }
    }
}