package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.annotations.Rename
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import kotlin.test.assertEquals

class TestEnumType {
    @Suppress("UNUSED")
    enum class EnumType {
        First,
        Second,
        Third,
    }

    @Suppress("UNUSED")
    enum class RenameEnumType {
        OriginalName,

        @Rename("renamed-name")
        RenamedName,
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @EnumSource(value = EnumType::class)
    fun `encode should accept EnumType when querying postgresql`(value: EnumType): Unit =
        runBlocking {
            val query = "SELECT $1 enum_col;"

            PgConnectionHelper.defaultConnection().use { conn ->
                conn.registerEnumType<EnumType>("enum_type")
                val fetchValue =
                    conn.createPreparedQuery(query)
                        .bind(value)
                        .fetchScalar<EnumType>()
                assertEquals(value, fetchValue)
            }
        }

    private suspend fun decodeTest(
        value: EnumType,
        isPrepared: Boolean,
    ) {
        val query = "SELECT '$value'::enum_type;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            conn.registerEnumType<EnumType>("enum_type")
            val fetchValue =
                if (isPrepared) {
                    conn.createPreparedQuery(query)
                } else {
                    conn.createQuery(query)
                }.fetchScalar<EnumType>()
            assertEquals(value, fetchValue)
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @EnumSource(value = EnumType::class)
    fun `decode should return EnumType when simple querying postgresql custom enum`(
        value: EnumType,
    ): Unit =
        runBlocking {
            decodeTest(value = value, isPrepared = false)
        }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @EnumSource(value = EnumType::class)
    fun `decode should return EnumType when extended querying postgresql custom enum`(
        value: EnumType,
    ): Unit =
        runBlocking {
            decodeTest(value = value, isPrepared = true)
        }

    companion object {
        @JvmStatic
        @BeforeAll
        fun setup(): Unit =
            runBlocking {
                PgConnectionHelper.defaultConnection().use { connection ->
                    connection.sendSimpleQuery(
                        """
                        DROP TYPE IF EXISTS public.enum_type;
                        CREATE TYPE public.enum_type AS ENUM
                        (
                            'First',
                            'Second',
                            'Third'
                        );
                        """.trimIndent(),
                    ).close()
                    connection.sendSimpleQuery(
                        """
                        DROP TYPE IF EXISTS public.rename_enum;
                        CREATE TYPE public.rename_enum AS ENUM
                        (
                            'OriginalName',
                            'renamed-name'
                        );
                        """.trimIndent(),
                    ).close()
                }
            }
    }
}
