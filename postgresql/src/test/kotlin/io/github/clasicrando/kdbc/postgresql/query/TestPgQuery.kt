package io.github.clasicrando.kdbc.postgresql.query

import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.exceptions.EmptyQueryResult
import io.github.clasicrando.kdbc.core.exceptions.RowParseError
import io.github.clasicrando.kdbc.core.exceptions.TooManyRows
import io.github.clasicrando.kdbc.core.query.RowParser
import io.github.clasicrando.kdbc.core.query.getIntOrThrow
import io.github.clasicrando.kdbc.core.query.getStringOrThrow
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.getInt
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TestPgQuery {
    data class Row(val intValue: Int, val stringValue: String)

    object GoodRowParserTest : RowParser<Row> {
        override fun fromRow(row: DataRow): Row {
            return Row(
                intValue = getIntOrThrow(row, "int_value"),
                stringValue = getStringOrThrow(row, "string_value"),
            )
        }
    }

    object BadRowParserTest : RowParser<Row> {
        override fun fromRow(row: DataRow): Row {
            return Row(
                intValue = row.getInt(3)!!,
                stringValue = getStringOrThrow(row, "string_value"),
            )
        }
    }

    object BadRowParserTest2 : RowParser<Row> {
        override fun fromRow(row: DataRow): Row {
            return Row(
                intValue = row.getInt("int_value")!!,
                stringValue = getStringOrThrow(row, "string_value"),
            )
        }
    }

    @JvmInline
    value class Id(val value: Long)

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
            connection.createQuery("SELECT 1").use {
                val scalar = it.fetchScalar<Int>()
                assertNotNull(scalar)
                assertEquals(1, scalar)
            }
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
    fun `fetchScalar should succeed when valid query with value class type`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            connection.createQuery("SELECT 69").use {
                val scalar = it.fetchScalar<Id>()
                assertNotNull(scalar)
                assertEquals(Id(69), scalar)
            }
        }
    }

    @Test
    fun `fetchScalar should fail when query returns a different type`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            connection.createQuery("SELECT 1").use {
                assertThrows<ColumnDecodeError> { it.fetchScalar<List<Int>>() }
            }
        }
    }

    @Test
    fun `fetchScalar should fail when query returns value that cannot be put into value class`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            connection.createQuery("SELECT 'Not ID'").use {
                assertThrows<ColumnDecodeError> { it.fetchScalar<Id>() }
            }
        }
    }

    @Test
    fun `fetchFirst should succeed when valid query with rowparser`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            connection.createQuery("SELECT $1 int_value, $2 string_value").use {
                val row = it.bind(INT_VALUE)
                    .bind(STRING_VALUE)
                    .fetchFirst(GoodRowParserTest)
                assertNotNull(row)
                assertEquals(INT_VALUE, row.intValue)
                assertEquals(STRING_VALUE, row.stringValue)
            }
        }
    }

    @Test
    fun `fetchFirst should fail when bad row parser`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            connection.createQuery("SELECT $1 int_value, $2 string_value").use {
                it.bind(INT_VALUE)
                it.bind(STRING_VALUE)
                assertThrows<RowParseError> { it.fetchFirst(BadRowParserTest) }
            }
        }
    }

    @Test
    fun `fetchSingle should succeed when valid query with rowparser`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            connection.createQuery("SELECT $1 int_value, $2 string_value").use {
                val row = it.bind(INT_VALUE)
                    .bind(STRING_VALUE)
                    .fetchSingle(GoodRowParserTest)
                assertNotNull(row)
                assertEquals(INT_VALUE, row.intValue)
                assertEquals(STRING_VALUE, row.stringValue)
            }
        }
    }

    @Test
    fun `fetchSingle should fail when no rows are returned`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            connection.createQuery(
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
            connection.createQuery(
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
            connection.createQuery(
                """
                    SELECT *
                    FROM (SELECT $1 int_value, $2 string_value) t
                    CROSS JOIN generate_series(1,2) s
                """.trimIndent()
            ).use {
                val rows = it.bind(INT_VALUE)
                    .bind(STRING_VALUE)
                    .fetchAll(GoodRowParserTest)
                assertEquals(2, rows.size)
                for (row in rows) {
                    assertEquals(INT_VALUE, row.intValue)
                    assertEquals(STRING_VALUE, row.stringValue)
                }
            }
        }
    }

    @Test
    fun `fetchAll should fail when unexpected exception is thrown`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            connection.createQuery(
                """
                    SELECT *
                    FROM (SELECT null int_value, $1 string_value) t
                    CROSS JOIN generate_series(1,2) s
                """.trimIndent()
            ).use {
                it.bind(STRING_VALUE)
                val exception = assertThrows<RowParseError> { it.fetchAll(BadRowParserTest2) }
                val suppressedExceptions = exception.suppressedExceptions
                assertEquals(1, suppressedExceptions.size)
                assertTrue(suppressedExceptions.first() is NullPointerException)
            }
        }
    }

    @Test
    fun `fetch should succeed when valid query and row parser`(): Unit = runBlocking {
        PgConnectionHelper.defaultConnection().use { connection ->
            connection.createQuery(
                """
                    SELECT *
                    FROM (SELECT $1 int_value, $2 string_value) t
                    CROSS JOIN generate_series(1,2) s
                """.trimIndent()
            ).use {
                val rows = it.bind(INT_VALUE)
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
    }

    companion object {
        const val INT_VALUE = 1
        const val STRING_VALUE = "test"
    }
}