package io.github.clasicrando.kdbc.postgresql.query

import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.exceptions.EmptyQueryResult
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.exceptions.RowParseError
import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetch
import io.github.clasicrando.kdbc.core.query.fetchAll
import io.github.clasicrando.kdbc.core.query.fetchFirst
import io.github.clasicrando.kdbc.core.query.fetchOne
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlin.test.Test
import kotlin.test.assertContains
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.assertThrows

class TestQuerySimple {
    data class Row(val intValue: Int, val stringValue: String)

    object GoodRowParserTest : RowParser<Row> {
        override fun fromRow(row: DataRow): Row =
            Row(
                intValue = row.getAsNonNull("int_value"),
                stringValue = row.getAsNonNull("string_value"),
            )
    }

    object BadRowParserTest : RowParser<Row> {
        override fun fromRow(row: DataRow): Row =
            Row(intValue = row.getAsNonNull(3), stringValue = row.getAsNonNull("string_value"))
    }

    object BadRowParserTest2 : RowParser<Row> {
        override fun fromRow(row: DataRow): Row =
            Row(
                intValue = row.getAsNonNull("int_value"),
                stringValue = row.getAsNonNull("string_value"),
            )
    }

    @Test
    fun `execute should succeed when valid query`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val query = query("SELECT 1")
            connection.executeQuery(query).collect()
        }
    }

    @Test
    fun `fetchScalar should succeed when valid query with basic type`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val scalar = query("SELECT 1").fetchScalar<Int>(connection)
            assertNotNull(scalar)
            assertEquals(1, scalar)
        }
    }

    @Test
    fun `fetchScalar should succeed when valid query with custom type`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val scalar = query("SELECT '{1,2,3}'::int[]").fetchScalar<List<Int>>(connection)
            assertNotNull(scalar)
            Assertions.assertIterableEquals(listOf(1, 2, 3), scalar)
        }
    }

    @Test
    fun `fetchScalar should fail when query returns a different type`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            assertThrows<ColumnDecodeError> { query("SELECT 1").fetchScalar<List<Int>>(connection) }
        }
    }

    @Test
    fun `fetchFirst should succeed when valid query with rowparser`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val row =
                query("SELECT $INT_VALUE int_value, '$STRING_VALUE' string_value")
                    .fetchFirst(connection, GoodRowParserTest)
            assertNotNull(row)
            assertEquals(INT_VALUE, row.intValue)
            assertEquals(STRING_VALUE, row.stringValue)
        }
    }

    @Test
    fun `fetchFirst should fail when bad row parser`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val query = query("SELECT $INT_VALUE int_value, '$STRING_VALUE' string_value")
            assertThrows<RowParseError> { query.fetchFirst(connection, BadRowParserTest) }
        }
    }

    @Test
    fun `fetchOne should succeed when valid query with rowparser`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val row =
                query("SELECT $INT_VALUE int_value, '$STRING_VALUE' string_value")
                    .fetchOne(connection, GoodRowParserTest)
            assertNotNull(row)
            assertEquals(INT_VALUE, row.intValue)
            assertEquals(STRING_VALUE, row.stringValue)
        }
    }

    @Test
    fun `fetchOne should fail when no rows are returned`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val query =
                query(
                    """
                        SELECT *
                        FROM (SELECT $INT_VALUE int_value, '$STRING_VALUE' string_value) t
                        WHERE 1 = 2
                        """
                        .trimIndent()
                )
            assertThrows<EmptyQueryResult> { query.fetchOne(connection, BadRowParserTest) }
        }
    }

    @Test
    fun `fetchAll should succeed when valid query and row parser`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val rows =
                query(
                        """
                        SELECT *
                        FROM (SELECT $INT_VALUE int_value, '$STRING_VALUE' string_value) t
                        CROSS JOIN generate_series(1,2) s
                        """
                            .trimIndent()
                    )
                    .fetchAll(connection, GoodRowParserTest)
            assertEquals(2, rows.size)
            for (row in rows) {
                assertEquals(INT_VALUE, row.intValue)
                assertEquals(STRING_VALUE, row.stringValue)
            }
        }
    }

    @Test
    fun `fetchAll should fail when unexpected exception is thrown`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val query =
                query(
                    """
                        SELECT *
                        FROM (SELECT null int_value, '$STRING_VALUE' string_value) t
                        CROSS JOIN generate_series(1,2) s
                        """
                        .trimIndent()
                )
            val exception =
                assertThrows<RowParseError> { query.fetchAll(connection, BadRowParserTest2) }
            exception.printStackTrace()
            val suppressedExceptions = exception.suppressedExceptions
            assertEquals(1, suppressedExceptions.size)
            val suppressedException = suppressedExceptions.first()
            assertTrue(
                suppressedException is KdbcException,
                "Actual exception: $suppressedException",
            )
            assertNotNull(suppressedException.message)
            assertContains(suppressedException.message!!, "null")
        }
    }

    @Test
    fun `fetch should succeed when valid query and row parser`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val rows =
                query(
                        """
                        SELECT *
                        FROM (SELECT $INT_VALUE int_value, '$STRING_VALUE' string_value) t
                        CROSS JOIN generate_series(1,2) s
                        """
                            .trimIndent()
                    )
                    .fetch(connection, GoodRowParserTest)
            var count = 0
            rows.collect { row ->
                count++
                assertEquals(INT_VALUE, row.intValue)
                assertEquals(STRING_VALUE, row.stringValue)
            }
            assertEquals(2, count)
        }
    }

    @Test
    fun `fetchFirst should succeed when valid query with rowparser and parameters`(): Unit =
        runBlocking {
            PgConnectionHelper.defaultConnection().use { connection ->
                val row =
                    query("SELECT $1 int_value, $2 string_value")
                        .bind(INT_VALUE)
                        .bind(STRING_VALUE)
                        .fetchFirst(connection, GoodRowParserTest)
                assertNotNull(row)
                assertEquals(INT_VALUE, row.intValue)
                assertEquals(STRING_VALUE, row.stringValue)
            }
        }

    @Test
    fun `fetchFirst should fail when bad row parser and parameters`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val query =
                query("SELECT $1 int_value, $2 string_value").bind(INT_VALUE).bind(STRING_VALUE)
            assertThrows<RowParseError> { query.fetchFirst(connection, BadRowParserTest) }
        }
    }

    @Test
    fun `fetchOne should succeed when valid query with rowparser and parameters`(): Unit =
        runBlocking {
            PgConnectionHelper.defaultConnection().use { connection ->
                val row =
                    query("SELECT $1 int_value, $2 string_value")
                        .bind(INT_VALUE)
                        .bind(STRING_VALUE)
                        .fetchOne(connection, GoodRowParserTest)
                assertNotNull(row)
                assertEquals(INT_VALUE, row.intValue)
                assertEquals(STRING_VALUE, row.stringValue)
            }
        }

    @Test
    fun `fetchOne should fail when no rows are returned and parameters`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val query =
                query(
                        """
                        SELECT *
                        FROM (SELECT $1 int_value, $2 string_value) t
                        WHERE 1 = 2
                        """
                            .trimIndent()
                    )
                    .bind(INT_VALUE)
                    .bind(STRING_VALUE)
            assertThrows<EmptyQueryResult> { query.fetchOne(connection, BadRowParserTest) }
        }
    }

    @Test
    fun `fetchAll should succeed when valid query and row parser and parameters`(): Unit =
        runBlocking {
            PgConnectionHelper.defaultConnection().use { connection ->
                val rows =
                    query(
                            """
                        SELECT *
                        FROM (SELECT $1 int_value, $2 string_value) t
                        CROSS JOIN generate_series(1,2) s
                        """
                                .trimIndent()
                        )
                        .bind(INT_VALUE)
                        .bind(STRING_VALUE)
                        .fetchAll(connection, GoodRowParserTest)
                assertEquals(2, rows.size)
                for (row in rows) {
                    assertEquals(INT_VALUE, row.intValue)
                    assertEquals(STRING_VALUE, row.stringValue)
                }
            }
        }

    @Test
    fun `fetchAll should fail when unexpected exception is thrown and parameters`(): Unit =
        runBlocking {
            PgConnectionHelper.defaultConnection().use { connection ->
                val rows =
                    query(
                            """
                        SELECT *
                        FROM (SELECT null int_value, $1 string_value) t
                        CROSS JOIN generate_series(1,2) s
                        """
                                .trimIndent()
                        )
                        .bind(STRING_VALUE)
                val exception =
                    assertThrows<RowParseError> { rows.fetchAll(connection, BadRowParserTest2) }
                exception.printStackTrace()
                val suppressedExceptions = exception.suppressedExceptions
                assertEquals(1, suppressedExceptions.size)
                val suppressedException = suppressedExceptions.first()
                assertTrue(
                    suppressedException is KdbcException,
                    "Actual exception: $suppressedException",
                )
                assertNotNull(suppressedException.message)
                assertContains(suppressedException.message!!, "null")
            }
        }

    @Test
    fun `fetch should succeed when valid query and row parser and parameters`(): Unit =
        runBlocking {
            PgConnectionHelper.defaultConnection().use { connection ->
                val rows =
                    query(
                            """
                        SELECT *
                        FROM (SELECT $1 int_value, $2 string_value) t
                        CROSS JOIN generate_series(1,2) s
                        """
                                .trimIndent()
                        )
                        .bind(INT_VALUE)
                        .bind(STRING_VALUE)
                        .fetch(connection, GoodRowParserTest)
                var count = 0
                rows.collect { row ->
                    count++
                    assertEquals(INT_VALUE, row.intValue)
                    assertEquals(STRING_VALUE, row.stringValue)
                }
                assertEquals(2, count)
            }
        }

    companion object {
        const val INT_VALUE = 1
        const val STRING_VALUE = "test"
    }
}
