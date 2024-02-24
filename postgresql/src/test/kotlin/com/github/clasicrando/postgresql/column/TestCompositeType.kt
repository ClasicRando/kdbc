package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.common.datetime.DateTime
import com.github.clasicrando.common.result.getAs
import com.github.clasicrando.postgresql.PgConnectionHelper
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
    fun `encode should accept CompositeTest when querying postgresql`() = runBlocking {
        val query = "SELECT $1 composite_col;"

        val result = PgConnectionHelper.defaultConnection().use {
            it.registerCompositeType<CompositeType>("composite_type")
            it.sendPreparedStatement(query, listOf(type))
        }.toList()

        assertEquals(1, result.size)
        assertEquals(1, result[0].rowsAffected)
        val rows = result[0].rows.toList()
        assertEquals(1, rows.size)
        assertEquals(type, rows.map { it.getAs<CompositeType>("composite_col") }.first())
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT row(1,'Composite Type','2024-02-25T05:25:51Z')::composite_type;"

        val result = PgConnectionHelper.defaultConnectionWithForcedSimple().use {
            it.registerCompositeType<CompositeType>("composite_type")
            if (isPrepared) {
                it.sendPreparedStatement(query, emptyList())
            } else {
                it.sendQuery(query)
            }
        }.toList()

        assertEquals(1, result.size)
        assertEquals(1, result[0].rowsAffected)
        val rows = result[0].rows.toList()
        assertEquals(1, rows.size)
        assertEquals(type, rows.map { it.getAs<CompositeType>(0)!! }.first())
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
