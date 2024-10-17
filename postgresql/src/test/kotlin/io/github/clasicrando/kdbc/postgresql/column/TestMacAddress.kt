package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.executeClosing
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.PgMacAddress
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class TestMacAddress {
    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `encode should accept PgMacAddress when querying postgresql`(isMacAddr8: Boolean) = runBlocking {
        val tableName = if (isMacAddr8) MACADDR8_TEST_TABLE else MACADDR_TEST_TABLE
        val query = "INSERT INTO public.$tableName(column_1) VALUES($1) RETURNING column_1"
        val value = if (isMacAddr8) macAddrValue else macAddrValue.toMacAddr()
        println(value.isMacAddress8)

        PgConnectionHelper.defaultConnection().use { conn ->
            val pgMacAddress = conn.createPreparedQuery(query)
                .bind(value)
                .fetchScalar<PgMacAddress>()
            assertNotNull(pgMacAddress)
            assertEquals(value, pgMacAddress)
        }
    }

    private suspend fun decodeTest(isMacAddr8: Boolean, isPrepared: Boolean) {
        val query = "SELECT ${if (isMacAddr8) "'$MAC_ADDR8_STRING'::macaddr8" else "'$MAC_ADDR_STRING'::macaddr"};"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val pgMacAddress = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<PgMacAddress>()
            assertNotNull(pgMacAddress)
            assertEquals(
                if (isMacAddr8) macAddrValue else macAddrValue.toMacAddr(),
                pgMacAddress,
            )
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `decode should return PgMacAddress when simple querying postgresql macaddr`(value: Boolean): Unit = runBlocking {
        decodeTest(isMacAddr8 = value, isPrepared = false)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `decode should return PgMacAddress when extended querying postgresql macaddr`(value: Boolean): Unit = runBlocking {
        decodeTest(isMacAddr8 = value, isPrepared = true)
    }

    companion object {
        private const val MACADDR_TEST_TABLE = "macaddr_test"
        private const val MACADDR8_TEST_TABLE = "macaddr8_test"
        private const val MAC_ADDR_STRING = "08:00:2b:03:04:05"
        private const val MAC_ADDR8_STRING = "08:00:2b:01:02:03:04:05"
        private val macAddrValue = PgMacAddress.fromString(MAC_ADDR8_STRING)

        @BeforeAll
        @JvmStatic
        fun createObjects(): Unit = runBlocking {
            PgConnectionHelper.defaultConnection().use {
                it.createQuery("DROP TABLE IF EXISTS public.$MACADDR_TEST_TABLE").executeClosing()
                it.createQuery("DROP TABLE IF EXISTS public.$MACADDR8_TEST_TABLE").executeClosing()
                it.createQuery("CREATE TABLE public.$MACADDR_TEST_TABLE(column_1 macaddr)").executeClosing()
                it.createQuery("CREATE TABLE public.$MACADDR8_TEST_TABLE(column_1 macaddr8)").executeClosing()
            }
        }

        @AfterAll
        @JvmStatic
        fun cleanObjects(): Unit = runBlocking {
            PgConnectionHelper.defaultConnection().use {
                it.createQuery("DROP TABLE IF EXISTS public.$MACADDR_TEST_TABLE").executeClosing()
                it.createQuery("DROP TABLE IF EXISTS public.$MACADDR8_TEST_TABLE").executeClosing()
            }
        }
    }
}
