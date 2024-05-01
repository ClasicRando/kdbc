package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
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
    fun `encode should accept EnumType when querying blocking postgresql`(value: EnumType) {
        val query = "SELECT $1 enum_col;"

        PgConnectionHelper.defaultBlockingConnection().use { conn ->
            conn.registerEnumType<EnumType>("enum_type")
            val enumValue = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<EnumType>()
            assertEquals(value, enumValue)
        }
    }

    private fun decodeBlockingTest(value: EnumType, isPrepared: Boolean) {
        val query = "SELECT '$value'::enum_type;"

        PgConnectionHelper.defaultBlockingConnectionWithForcedSimple().use { conn ->
            conn.registerEnumType<EnumType>("enum_type")
            val enumValue = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<EnumType>()
            assertEquals(value, enumValue)
        }
    }

    @ParameterizedTest
    @EnumSource(value = EnumType::class)
    fun `decode should return EnumType when simple querying blocking postgresql custom enum`(value: EnumType) {
        decodeBlockingTest(value = value, isPrepared = false)
    }

    @ParameterizedTest
    @EnumSource(value = EnumType::class)
    fun `decode should return EnumType when extended querying blocking postgresql custom enum`(value: EnumType) {
        decodeBlockingTest(value = value, isPrepared = true)
    }

    @ParameterizedTest
    @EnumSource(value = EnumType::class)
    fun `encode should accept EnumType when querying postgresql`(value: EnumType) = runBlocking {
        val query = "SELECT $1 enum_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.registerEnumType<EnumType>("enum_type")
            val fetchValue = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<EnumType>()
            assertEquals(value, fetchValue)
        }
    }

    private suspend fun decodeTest(value: EnumType, isPrepared: Boolean) {
        val query = "SELECT '$value'::enum_type;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            conn.registerEnumType<EnumType>("enum_type")
            val fetchValue = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<EnumType>()
            assertEquals(value, fetchValue)
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
                connection.sendSimpleQuery("""
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
