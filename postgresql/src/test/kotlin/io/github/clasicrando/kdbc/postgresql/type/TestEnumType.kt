package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.annotations.Rename
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.execute
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.query.query
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlin.test.assertEquals
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

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
        @Rename("renamed-name") RenamedName,
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @EnumSource(value = EnumType::class)
    fun `encode should accept EnumType when querying postgresql`(value: EnumType): Unit =
        runBlocking {
            val query = "SELECT $1 enum_col;"

            PgConnectionHelper.defaultConnection().use { conn ->
                conn.registerEnumType<EnumType>("enum_type")
                val fetchValue = query(query).bind(value).fetchScalar<EnumType>(conn)
                assertEquals(value, fetchValue)
            }
        }

    private suspend fun decodeTest(value: EnumType, isExtended: Boolean) {
        val query = "SELECT '$value'::enum_type;"
        if (isExtended) {
                PgConnectionHelper.defaultConnection()
            } else {
                PgConnectionHelper.defaultConnectionWithForcedSimple()
            }
            .use { conn ->
                conn.registerEnumType<EnumType>("enum_type")
                val fetchValue = query(query).fetchScalar<EnumType>(conn)
                assertEquals(value, fetchValue)
            }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @EnumSource(value = EnumType::class)
    fun `decode should return EnumType when simple querying postgresql custom enum`(
        value: EnumType
    ): Unit = runBlocking { decodeTest(value = value, isExtended = false) }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @EnumSource(value = EnumType::class)
    fun `decode should return EnumType when extended querying postgresql custom enum`(
        value: EnumType
    ): Unit = runBlocking { decodeTest(value = value, isExtended = true) }

    companion object {
        @JvmStatic
        @BeforeAll
        fun setup(): Unit = runBlocking {
            PgConnectionHelper.defaultConnection().use { connection ->
                query(
                        """
                        DROP TYPE IF EXISTS public.enum_type;
                        CREATE TYPE public.enum_type AS ENUM
                        (
                            'First',
                            'Second',
                            'Third'
                        );
                        """
                            .trimIndent()
                    )
                    .execute(connection)
                query(
                        """
                        DROP TYPE IF EXISTS public.rename_enum;
                        CREATE TYPE public.rename_enum AS ENUM
                        (
                            'OriginalName',
                            'renamed-name'
                        );
                        """
                            .trimIndent()
                    )
                    .execute(connection)
            }
        }
    }
}
