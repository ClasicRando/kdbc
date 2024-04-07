package com.github.kdbc.postgresql.query

import com.github.kdbc.core.connection.use
import com.github.kdbc.core.exceptions.EmptyQueryResult
import com.github.kdbc.core.exceptions.IncorrectScalarType
import com.github.kdbc.core.exceptions.RowParseError
import com.github.kdbc.core.exceptions.TooManyRows
import com.github.kdbc.core.query.RowParser
import com.github.kdbc.core.query.getIntOrThrow
import com.github.kdbc.core.query.getStringOrThrow
import com.github.kdbc.core.result.DataRow
import com.github.kdbc.core.result.getInt
import com.github.kdbc.core.use
import com.github.kdbc.postgresql.PgConnectionHelper
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TestPgBlockingQuery {
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

    @Test
    fun `execute should succeed when valid query`() {
        PgConnectionHelper.defaultBlockingConnection().use { connection ->
            connection.createQuery("SELECT 1").use {
                it.execute()
            }
        }
    }

    @Test
    fun `fetchScalar should succeed when valid query with basic type`() {
        PgConnectionHelper.defaultBlockingConnection().use { connection ->
            connection.createQuery("SELECT 1").use {
                val scalar = it.fetchScalar<Int>()
                assertNotNull(scalar)
                assertEquals(1, scalar)
            }
        }
    }

    @Test
    fun `fetchScalar should succeed when valid query with custom type`() {
        PgConnectionHelper.defaultBlockingConnection().use { connection ->
            connection.createQuery("SELECT '{1,2,3}'::int[]").use {
                val scalar = it.fetchScalar<List<Int>>()
                assertNotNull(scalar)
                Assertions.assertIterableEquals(listOf(1,2,3), scalar)
            }
        }
    }

    @Test
    fun `fetchScalar should fail when query returns a different type`() {
        PgConnectionHelper.defaultBlockingConnection().use { connection ->
            connection.createQuery("SELECT 1").use {
                assertThrows<IncorrectScalarType> { it.fetchScalar<List<Int>>() }
            }
        }
    }

    @Test
    fun `fetchFirst should succeed when valid query with rowparser`() {
        PgConnectionHelper.defaultBlockingConnection().use { connection ->
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
    fun `fetchFirst should fail when bad row parser`() {
        PgConnectionHelper.defaultBlockingConnection().use { connection ->
            connection.createQuery("SELECT $1 int_value, $2 string_value").use {
                it.bind(INT_VALUE)
                it.bind(STRING_VALUE)
                assertThrows<RowParseError> { it.fetchFirst(BadRowParserTest) }
            }
        }
    }

    @Test
    fun `fetchSingle should succeed when valid query with rowparser`() {
        PgConnectionHelper.defaultBlockingConnection().use { connection ->
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
    fun `fetchSingle should fail when no rows are returned`() {
        PgConnectionHelper.defaultBlockingConnection().use { connection ->
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
    fun `fetchSingle should fail when multiple rows are returned`() {
        PgConnectionHelper.defaultBlockingConnection().use { connection ->
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
    fun `fetchAll should succeed when valid query and row parser`() {
        PgConnectionHelper.defaultBlockingConnection().use { connection ->
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
    fun `fetchAll should fail when unexpected exception is thrown`() {
        PgConnectionHelper.defaultBlockingConnection().use { connection ->
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
    fun `fetch should succeed when valid query and row parser`() {
        PgConnectionHelper.defaultBlockingConnection().use { connection ->
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
                for (row in rows) {
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