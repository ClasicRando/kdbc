package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.common.result.getAs
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import kotlin.test.assertEquals

class TestEnumType {
    enum class EnumType {
        First,
        Second,
        Third,
    }

    @ParameterizedTest
    @EnumSource(value = EnumType::class)
    fun `encode should accept EnumType when querying postgresql`(value: EnumType) = runBlocking {
        val query = "SELECT $1 enum_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.registerEnumType<EnumType>("enum_type")
            conn.sendPreparedStatement(query, listOf(value)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(value, rows.map { it.getAs<EnumType>("enum_col") }.first())
            }
        }
    }

    private suspend fun decodeTest(value: EnumType, isPrepared: Boolean) {
        val query = "SELECT '$value'::enum_type;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            conn.registerEnumType<EnumType>("enum_type")
            if (isPrepared) {
                conn.sendPreparedStatement(query, emptyList())
            } else {
                conn.sendQuery(query)
            }.use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(value, rows.map { it.getAs<EnumType>(0)!! }.first())
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = EnumType::class)
    fun `decode should return EnumType when simple querying postgresql custom enum`(value: EnumType): Unit = runBlocking {
        decodeTest(value = value, isPrepared = false)
    }

    @ParameterizedTest
    @EnumSource(value = EnumType::class)
    fun `decode should return EnumType when extended querying postgresql custom enum`(value: EnumType): Unit = runBlocking {
        decodeTest(value = value, isPrepared = true)
    }

    companion object {
        @JvmStatic
        @BeforeAll
        fun setup(): Unit = runBlocking {
            PgConnectionHelper.defaultConnection().use { connection ->
                connection.sendQuery("""
                    DROP TYPE IF EXISTS public.enum_type;
                    CREATE TYPE public.enum_type AS ENUM
                    (
                        'First',
                        'Second',
                        'Third'
                    );
                """.trimIndent()).release()
            }
        }
    }
}
