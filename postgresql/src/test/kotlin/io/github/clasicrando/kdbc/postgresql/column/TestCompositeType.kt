package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.datetime.DateTime
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
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
            val value = conn.createPreparedQuery(query)
                .bind(type)
                .fetchScalar<CompositeType>()
            assertEquals(type, value)
        }
    }

    private fun decodeBlockingTest(isPrepared: Boolean) {
        val query = "SELECT row(1,'Composite Type','2024-02-25T05:25:51Z')::composite_type;"

        PgConnectionHelper.defaultBlockingConnectionWithForcedSimple().use { conn ->
            conn.registerCompositeType<CompositeType>("composite_type")
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<CompositeType>()
            assertEquals(type, value)
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

        PgConnectionHelper.defaultAsyncConnection().use { conn ->
            conn.registerCompositeType<CompositeType>("composite_type")
            val value = conn.createPreparedQuery(query)
                .bind(type)
                .fetchScalar<CompositeType>()
            assertEquals(type, value)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT row(1,'Composite Type','2024-02-25T05:25:51Z')::composite_type;"

        PgConnectionHelper.defaultAsyncConnectionWithForcedSimple().use { conn ->
            conn.registerCompositeType<CompositeType>("composite_type")
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<CompositeType>()
            assertEquals(type, value)
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
            PgConnectionHelper.defaultAsyncConnection().use { connection ->
                connection.sendSimpleQuery("""
                    DROP TYPE IF EXISTS public.composite_type;
                    CREATE TYPE public.composite_type AS
                    (
                        id int,
                        "text" text,
                        "timestamp" timestamptz
                    );
                """.trimIndent()).close()
            }
        }
    }
}
