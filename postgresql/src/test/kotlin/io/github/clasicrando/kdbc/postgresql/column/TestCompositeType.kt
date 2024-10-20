package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.annotations.Rename
import io.github.clasicrando.kdbc.core.datetime.DateTime
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.result.DataRow
import io.github.clasicrando.kdbc.core.result.getAsNonNull
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.CompositeTypeDefinition
import io.github.clasicrando.kdbc.postgresql.type.PgJson
import kotlinx.coroutines.runBlocking
import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalTime
import kotlinx.datetime.UtcOffset
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Timeout
import kotlin.reflect.KType
import kotlin.reflect.typeOf
import kotlin.test.Test
import kotlin.test.assertEquals

class TestCompositeType {
    data class CompositeType(val id: Int, val text: String, val timestamp: DateTime)

    data class CompositeTable(
        val id: Int,
        @Rename("record_order")
        val recordOrder: Int,
        @Rename("sub_id")
        val subId: Long,
        @Rename("json_value")
        val jsonValue: PgJson?,
    )

    data class CompositeDef(
        val id: Int,
        val text: String,
    ) {
        companion object : CompositeTypeDefinition<CompositeDef> {
            override fun extractValues(value: CompositeDef): List<Pair<Any?, KType>> {
                return listOf(
                    value.id to typeOf<Int>(),
                    value.text to typeOf<String>(),
                )
            }

            override fun fromRow(row: DataRow): CompositeDef {
                return CompositeDef(
                    id = row.getAsNonNull("id"),
                    text = row.getAsNonNull("text"),
                )
            }
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept CompositeTest when querying postgresql`(): Unit =
        runBlocking {
            val query = "SELECT $1 composite_col;"

            PgConnectionHelper.defaultConnection().use { conn ->
                conn.registerCompositeType<CompositeType>("composite_type")
                val value =
                    conn.createPreparedQuery(query)
                        .bind(type)
                        .fetchScalar<CompositeType>()
                assertEquals(type, value)
            }
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept CompositeTable when querying postgresql`(): Unit =
        runBlocking {
            val query = "SELECT $1 composite_table;"

            PgConnectionHelper.defaultConnection().use { conn ->
                conn.registerCompositeType<CompositeTable>("table_composite")
                val value =
                    conn.createPreparedQuery(query)
                        .bind(table)
                        .fetchScalar<CompositeTable>()
                assertEquals(table, value)
            }
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept CompositeDef when querying postgresql`(): Unit =
        runBlocking {
            val query = "SELECT $1 composite_def;"

            PgConnectionHelper.defaultConnection().use { conn ->
                conn.registerCompositeType<CompositeDef>("composite_def")
                val value =
                    conn.createPreparedQuery(query)
                        .bind(def)
                        .fetchScalar<CompositeDef>()
                assertEquals(def, value)
            }
        }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val query = "SELECT row(1,'Composite Type','2024-02-25T05:25:51Z')::composite_type;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            conn.registerCompositeType<CompositeType>("composite_type")
            val value =
                if (isPrepared) {
                    conn.createPreparedQuery(query)
                } else {
                    conn.createQuery(query)
                }.fetchScalar<CompositeType>()
            assertEquals(type, value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return CompositeType when simple querying postgresql composite`(): Unit =
        runBlocking {
            decodeTest(isPrepared = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return CompositeType when extended querying postgresql composite`(): Unit =
        runBlocking {
            decodeTest(isPrepared = true)
        }

    private suspend fun decodeTableTest(isPrepared: Boolean) {
        val query = "SELECT row(1,1,2930,NULL)::table_composite;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            conn.registerCompositeType<CompositeTable>("table_composite")
            val value =
                if (isPrepared) {
                    conn.createPreparedQuery(query)
                } else {
                    conn.createQuery(query)
                }.fetchScalar<CompositeTable>()
            assertEquals(table, value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return CompositeTable when simple querying postgresql composite`(): Unit =
        runBlocking {
            decodeTableTest(isPrepared = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return CompositeTable when extended querying postgresql composite`(): Unit =
        runBlocking {
            decodeTableTest(isPrepared = true)
        }

    private suspend fun decodeDefTest(isPrepared: Boolean) {
        val query = "SELECT row(1,'Composite Def')::composite_def;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.registerCompositeType<CompositeDef>("composite_def")
            val value =
                if (isPrepared) {
                    conn.createPreparedQuery(query)
                } else {
                    conn.createQuery(query)
                }.fetchScalar<CompositeDef>()
            assertEquals(def, value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return CompositeDef when simple querying postgresql composite`(): Unit =
        runBlocking {
            decodeDefTest(isPrepared = false)
        }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return CompositeDef when extended querying postgresql composite`(): Unit =
        runBlocking {
            decodeDefTest(isPrepared = true)
        }

    companion object {
        private val type =
            CompositeType(
                id = 1,
                text = "Composite Type",
                timestamp =
                    DateTime(
                        date = LocalDate(2024, 2, 25),
                        time = LocalTime(5, 25, 51),
                        offset = UtcOffset(seconds = 0),
                    ),
            )
        private val table =
            CompositeTable(
                id = 1,
                recordOrder = 1,
                subId = 2930,
                jsonValue = null,
            )
        private val def =
            CompositeDef(
                id = 1,
                text = "Composite Def",
            )

        @JvmStatic
        @BeforeAll
        fun setup(): Unit =
            runBlocking {
                PgConnectionHelper.defaultConnection().use { connection ->
                    connection.sendSimpleQuery(
                        """
                        DROP TYPE IF EXISTS public.composite_type;
                        CREATE TYPE public.composite_type AS
                        (
                            id int,
                            "text" text,
                            "timestamp" timestamptz
                        );
                        """.trimIndent(),
                    ).close()
                    connection.sendSimpleQuery(
                        """
                        DROP TABLE IF EXISTS public.table_composite;
                        CREATE TABLE public.table_composite
                        (
                            id int not null,
                            record_order integer not null,
                            sub_id bigint,
                            json_value jsonb
                        );
                        """.trimIndent(),
                    ).close()
                    connection.sendSimpleQuery(
                        """
                        DROP TYPE IF EXISTS public.composite_def;
                        CREATE TYPE public.composite_def AS
                        (
                            id int,
                            "text" text
                        );
                        """.trimIndent(),
                    ).close()
                }
            }
    }
}
