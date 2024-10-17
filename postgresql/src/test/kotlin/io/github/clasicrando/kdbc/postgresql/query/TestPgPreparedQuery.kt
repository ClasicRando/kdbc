package io.github.clasicrando.kdbc.postgresql.query

import io.github.clasicrando.kdbc.core.exceptions.EmptyQueryResult
import io.github.clasicrando.kdbc.core.exceptions.IncorrectScalarType
import io.github.clasicrando.kdbc.core.exceptions.RowParseError
import io.github.clasicrando.kdbc.core.exceptions.TooManyRows
import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetch
import io.github.clasicrando.kdbc.core.query.fetchAll
import io.github.clasicrando.kdbc.core.query.fetchFirst
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.fetchSingle
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TestPgPreparedQuery {
    data class Row(val intValue: Int, val stringValue: String)

    object GoodRowParserTest : RowParser<Row> {
        override fun fromRow(row: DataRow): Row {
            return Row(
                intValue = row.getAsNonNull("int_value"),
                stringValue = row.getAsNonNull("string_value"),
            )
        }
    }

    object BadRowParserTest : RowParser<Row> {
        override fun fromRow(row: DataRow): Row {
            return Row(
                intValue = row.getAsNonNull(3),
                stringValue = row.getAsNonNull("string_value"),
            )
        }
    }

    object BadRowParserTest2 : RowParser<Row> {
        override fun fromRow(row: DataRow): Row {
            return Row(
                intValue = row.getAsNonNull("int_value"),
                stringValue = row.getAsNonNull("string_value"),
            )
        }
    }

    @Test
    fun `execute should succeed when valid query`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            connection.createQuery("SELECT 1").use {
                it.execute()
            }
        }
    }

    @Test
    fun `fetchScalar should succeed when valid query with basic type`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val scalar = connection.createQuery("SELECT 1").fetchScalar<Int>()
            assertNotNull(scalar)
            assertEquals(1, scalar)
        }
    }

    @Test
    fun `fetchScalar should succeed when valid query with custom type`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            connection.createQuery("SELECT '{1,2,3}'::int[]").use {
                val scalar = it.fetchScalar<List<Int>>()
                assertNotNull(scalar)
                Assertions.assertIterableEquals(listOf(1,2,3), scalar)
            }
        }
    }

    @Test
    fun `fetchScalar should fail when query returns a different type`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            assertThrows<IncorrectScalarType> {
                connection.createQuery("SELECT 1").fetchScalar<List<Int>>()
            }
        }
    }

    @Test
    fun `fetchFirst should succeed when valid query with rowparser`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val row = connection.createPreparedQuery("SELECT $1 int_value, $2 string_value")
                .bind(INT_VALUE)
                .bind(STRING_VALUE)
                .fetchFirst(GoodRowParserTest)
            assertNotNull(row)
            assertEquals(INT_VALUE, row.intValue)
            assertEquals(STRING_VALUE, row.stringValue)
        }
    }

    @Test
    fun `fetchFirst should fail when bad row parser`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            connection.createPreparedQuery("SELECT $1 int_value, $2 string_value").use {
                it.bind(INT_VALUE)
                it.bind(STRING_VALUE)
                assertThrows<RowParseError> { it.fetchFirst(BadRowParserTest) }
            }
        }
    }

    @Test
    fun `fetchSingle should succeed when valid query with rowparser`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val row = connection.createPreparedQuery("SELECT $1 int_value, $2 string_value")
                .bind(INT_VALUE)
                .bind(STRING_VALUE)
                .fetchSingle(GoodRowParserTest)
            assertNotNull(row)
            assertEquals(INT_VALUE, row.intValue)
            assertEquals(STRING_VALUE, row.stringValue)
        }
    }

    @Test
    fun `fetchSingle should fail when no rows are returned`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            connection.createPreparedQuery(
                """
                    SELECT *
                    FROM (SELECT $1 int_value, $2 string_value) t
                    WHERE 1 = 2
                """.trimIndent()
            ).use {
                it.bind(INT_VALUE)
                it.bind(STRING_VALUE)
                assertThrows<EmptyQueryResult> { it.fetchSingle(BadRowParserTest) }
            }
        }
    }

    @Test
    fun `fetchSingle should fail when multiple rows are returned`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            connection.createPreparedQuery(
                """
                    SELECT *
                    FROM (SELECT $1 int_value, $2 string_value) t
                    CROSS JOIN generate_series(1,2) s
                """.trimIndent()
            ).use {
                it.bind(INT_VALUE)
                it.bind(STRING_VALUE)
                assertThrows<TooManyRows> { it.fetchSingle(BadRowParserTest) }
            }
        }
    }

    @Test
    fun `fetchAll should succeed when valid query and row parser`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val rows = connection.createPreparedQuery(
                """
                    SELECT *
                    FROM (SELECT $1 int_value, $2 string_value) t
                    CROSS JOIN generate_series(1,2) s
                """.trimIndent()
            )
                .bind(INT_VALUE)
                .bind(STRING_VALUE)
                .fetchAll(GoodRowParserTest)
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
            val rows = connection.createPreparedQuery(
                """
                    SELECT *
                    FROM (SELECT null int_value, $1 string_value) t
                    CROSS JOIN generate_series(1,2) s
                """.trimIndent()
            )
                .bind(STRING_VALUE)
            val exception = assertThrows<RowParseError> { rows.fetchAll(BadRowParserTest2) }
            val suppressedExceptions = exception.suppressedExceptions
            assertEquals(1, suppressedExceptions.size)
            assertTrue(suppressedExceptions.first() is NullPointerException)
        }
    }

    @Test
    fun `fetch should succeed when valid query and row parser`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            val rows = connection.createPreparedQuery(
                """
                    SELECT *
                    FROM (SELECT $1 int_value, $2 string_value) t
                    CROSS JOIN generate_series(1,2) s
                """.trimIndent()
            )
                .bind(INT_VALUE)
                .bind(STRING_VALUE)
                .fetch(GoodRowParserTest)
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