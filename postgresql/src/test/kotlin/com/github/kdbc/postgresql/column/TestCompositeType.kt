package com.github.kdbc.postgresql.column

import com.github.kdbc.core.connection.use
import com.github.kdbc.core.datetime.DateTime
import com.github.kdbc.core.result.getAs
import com.github.kdbc.core.use
import com.github.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset
import org.junit.jupiter.api.BeforeAll
import kotlin.test.Test
import kotlin.test.assertEquals

class TestCompositeType {
    data class CompositeType(val id: Int, val text: String, val timestamp: DateTime)

    @Test
    fun `encode should accept CompositeTest when querying blocking postgresql`() {
        val query = "SELECT $1 composite_col;"

        PgConnectionHelper.defaultBlockingConnection().use { conn ->
            conn.registerCompositeType<CompositeType>("composite_type")
            conn.sendPreparedStatement(query, listOf(type)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(type, rows.map { it.getAs<CompositeType>("composite_col") }.first())
            }
        }
    }

    private fun decodeBlockingTest(isPrepared: Boolean) {
        val query = "SELECT row(1,'Composite Type','2024-02-25T05:25:51Z')::composite_type;"

        PgConnectionHelper.defaultBlockingConnectionWithForcedSimple().use { conn ->
            conn.registerCompositeType<CompositeType>("composite_type")
            if (isPrepared) {
                conn.sendPreparedStatement(query, emptyList())
            } else {
                conn.sendQuery(query)
            }.use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(type, rows.map { it.getAs<CompositeType>(0)!! }.first())
            }
        }
    }

    @Test
    fun `decode should return CompositeType when simple querying blocking postgresql composite`() {
        decodeBlockingTest(isPrepared = false)
    }

    @Test
    fun `decode should return CompositeType when extended querying blocking postgresql composite`() {
        decodeBlockingTest(isPrepared = true)
    }

    @Test
    fun `encode should accept CompositeTest when querying postgresql`() = runBlocking {
        val query = "SELECT $1 composite_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.registerCompositeType<CompositeType>("composite_type")
            conn.sendPreparedStatement(query, listOf(type)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(type, rows.map { it.getAs<CompositeType>("composite_col") }.first())
            }
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT row(1,'Composite Type','2024-02-25T05:25:51Z')::composite_type;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            conn.registerCompositeType<CompositeType>("composite_type")
            if (isPrepared) {
                conn.sendPreparedStatement(query, emptyList())
            } else {
                conn.sendQuery(query)
            }.use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(type, rows.map { it.getAs<CompositeType>(0)!! }.first())
            }
        }
    }

    @Test
    fun `decode should return CompositeType when simple querying postgresql composite`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return CompositeType when extended querying postgresql composite`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }

    companion object {
        private val type = CompositeType(
            id = 1,
            text = "Composite Type",
            timestamp = DateTime(
                date = LocalDate(2024, 2, 25),
                time = LocalTime(5, 25, 51),
                offset = UtcOffset(seconds = 0),
            )
        )

        @JvmStatic
        @BeforeAll
        fun setup(): Unit = runBlocking {
            PgConnectionHelper.defaultConnection().use { connection ->
                connection.sendQuery("""
                    DROP TYPE IF EXISTS public.composite_type;
                    CREATE TYPE public.composite_type AS
                    (
                        id int,
                        "text" text,
                        "timestamp" timestamptz
                    );
                """.trimIndent()).release()
            }
        }
    }
}
