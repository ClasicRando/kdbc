package io.github.clasicrando.kdbc.mysql.connection

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.bindOut
import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.core.useCatching
import io.github.clasicrando.kdbc.mysql.MySqlConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TestQuerySpec {
    @BeforeTest
    fun init(): Unit = runBlocking {
        MySqlConnectionHelper.defaultConnection().use { query(INIT_SCRIPT).execute(it) }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `executeQuery should return single row when simple query`(): Unit = runBlocking {
        val simpleQuery = query("SELECT 1 AS test")
        MySqlConnectionHelper.defaultConnection().use {
            val (results, resultGroups) = it.executeQuery(simpleQuery).collectResults()
            assertEquals(1, results.size)
            assertEquals(1, results[0].rowsAffected)

            assertEquals(1, resultGroups.size)
            val rows = resultGroups[0]
            assertEquals(1, rows.size)
            assertEquals(1, rows[0].getAsNonNull("test"))
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `executeQuery should return multiple row when simple query`(): Unit = runBlocking {
        val simpleQuery = query("SELECT 1 AS test UNION SELECT 2")
        MySqlConnectionHelper.defaultConnection().use {
            val (results, resultGroups) = it.executeQuery(simpleQuery).collectResults()
            assertEquals(1, results.size)
            assertEquals(2, results[0].rowsAffected)

            assertEquals(1, resultGroups.size)
            val rows = resultGroups[0]
            assertEquals(2, rows.size)
            assertEquals(1, rows[0].getAsNonNull("test"))
            assertEquals(2, rows[1].getAsNonNull("test"))
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `executeQuery should return multiple results when multi query`(): Unit = runBlocking {
        val simpleQuery = query("SELECT 1 AS test; SELECT 2 AS `second row`")
        MySqlConnectionHelper.defaultConnection().use {
            val (results, resultGroups) = it.executeQuery(simpleQuery).collectResults()
            assertEquals(2, results.size)
            assertEquals(1, results[0].rowsAffected)
            assertEquals(1, results[1].rowsAffected)

            assertEquals(2, resultGroups.size)
            val rows1 = resultGroups[0]
            assertEquals(1, rows1.size)
            assertEquals(1, rows1[0].getAsNonNull("test"))
            val rows2 = resultGroups[1]
            assertEquals(1, rows2.size)
            assertEquals(2, rows2[0].getAsNonNull("second row"))
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `executeQuery should timeout when long running query`(): Unit = runBlocking {
        val timeoutQuery =
            query(
                """
            SET @@cte_max_recursion_depth = 1000000000000;
            WITH RECURSIVE cte (id) AS (
                SELECT 1 AS id
                UNION ALL
                SELECT id + 1
                FROM cte
            )
            SELECT *
            FROM cte;
        """
                    .trimIndent()
            )
        val result =
            MySqlConnectionHelper.defaultConnectionWithTimeout().useCatching { conn ->
                conn.executeQuery(timeoutQuery).collectResults()
            }
        assertTrue(result.isFailure)
        val exception = result.exceptionOrNull()
        assertNotNull(exception)
        assertContains(
            exception.message!!,
            "Query execution was interrupted, maximum statement execution time exceeded",
        )
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `executeQuery should return single row when prepared statement`(): Unit = runBlocking {
        val preparedStatement = query("SELECT ? AS test").bind(1)
        MySqlConnectionHelper.defaultConnection().use {
            val (results, resultGroups) = it.executeQuery(preparedStatement).collectResults()
            assertEquals(1, results.size)
            assertEquals(1, results[0].rowsAffected)

            assertEquals(1, resultGroups.size)
            val rows = resultGroups[0]
            assertEquals(1, rows.size)
            assertEquals(1, rows[0].getAsNonNull("test"))
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `executeQuery should return multiple row when prepared statement`(): Unit = runBlocking {
        val simpleQuery = query("SELECT ? AS test UNION SELECT ?").bind(1).bind(2)
        MySqlConnectionHelper.defaultConnection().use {
            val (results, resultGroups) = it.executeQuery(simpleQuery).collectResults()
            assertEquals(1, results.size)
            assertEquals(2, results[0].rowsAffected)

            assertEquals(1, resultGroups.size)
            val rows = resultGroups[0]
            assertEquals(2, rows.size)
            assertEquals(1, rows[0].getAsNonNull("test"))
            assertEquals(2, rows[1].getAsNonNull("test"))
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `executeQuery should return single row when stored procedure with out parameter`(): Unit =
        runBlocking {
            val preparedStatement = query("CALL $TEST_PROC_NAME(1, ?)").bindOut<String>()
            MySqlConnectionHelper.defaultConnection().use {
                val (results, resultGroups) = it.executeQuery(preparedStatement).collectResults()
                assertEquals(2, results.size)
                assertEquals(1, results[0].rowsAffected)
                assertEquals(0, results[1].rowsAffected)

                assertEquals(2, resultGroups.size)
                val rows = resultGroups[0]
                assertEquals(1, rows.size)
                assertEquals("This is a test", rows[0].getAsNonNull("text_value"))
                assertEquals(0, resultGroups[1].size)
            }
        }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @CsvSource("true,true", "true,false", "false,true", "false,false")
    fun `executeQueryBatch should return multiple results when multiple queries`(
        inTransaction: Boolean,
        shouldSucceed: Boolean,
    ): Unit = runBlocking {
        val expectedCount =
            if (shouldSucceed) {
                2
            } else if (!inTransaction) {
                1
            } else {
                0
            }
        val infallible = query("INSERT INTO $TEST_TABLE VALUES (1);")
        val fallible =
            if (shouldSucceed) {
                query("INSERT INTO $TEST_TABLE VALUES(?)").bind(2)
            } else {
                query("INSERT INTO $TEST_TABLE VALUES(?)").bind("Test")
            }
        val queries = listOf(infallible, fallible, query("SELECT ? AS `second row`").bind(2))
        val result =
            MySqlConnectionHelper.defaultConnection().useCatching {
                it.executeQueryBatch(queries, inTransaction).collectResults()
            }

        assertEquals(
            shouldSucceed,
            result.isSuccess,
            result.exceptionOrNull()?.stackTraceToString(),
        )

        if (result.isSuccess) {
            val (results, resultGroups) = result.getOrThrow()
            assertEquals(3, results.size)
            assertEquals(1, results[0].rowsAffected)
            assertEquals(1, results[1].rowsAffected)
            assertEquals(1, results[2].rowsAffected)

            assertEquals(3, resultGroups.size)
            assertEquals(0, resultGroups[0].size)
            assertEquals(0, resultGroups[1].size)
            val rows = resultGroups[2]
            assertEquals(1, rows.size)
            assertEquals(2, rows[0].getAsNonNull("second row"))
        }

        MySqlConnectionHelper.defaultConnection().use {
            val count = query("SELECT COUNT(*) FROM $TEST_TABLE").fetchScalar<Int>(it)
            assertNotNull(count)
            assertEquals(expectedCount, count)
        }
    }

    companion object {
        const val TEST_PROC_NAME = "test_proc"
        const val TEST_TABLE = "test"
        private const val STARTUP_SCRIPT =
            """
            DROP PROCEDURE IF EXISTS $TEST_PROC_NAME;
            CREATE PROCEDURE $TEST_PROC_NAME(in int_value int, out text_value varchar(255))
            NO SQL
            BEGIN
                SET text_value = 'This is a test';
            END;
            """
        private const val INIT_SCRIPT =
            """
            DROP TABLE IF EXISTS $TEST_TABLE;
            CREATE TABLE $TEST_TABLE(id int);
            """

        @JvmStatic
        @BeforeAll
        fun setup(): Unit = runBlocking {
            MySqlConnectionHelper.defaultConnection().use { query(STARTUP_SCRIPT).execute(it) }
        }
    }
}
