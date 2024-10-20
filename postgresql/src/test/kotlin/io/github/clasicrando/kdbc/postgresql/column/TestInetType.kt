package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.PgInet
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import kotlin.test.assertEquals

class TestInetType {
    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @CsvSource(IPV4_ADDRESS_STRING, IPV6_ADDRESS_STRING)
    fun `encode should accept PgInet when querying postgresql`(inetAddress: String): Unit =
        runBlocking {
            val inet = PgInet.parse(inetAddress)
            val query = "SELECT $1 inet_col;"

            PgConnectionHelper.defaultConnection().use { conn ->
                val value =
                    conn.createPreparedQuery(query)
                        .bind(inet)
                        .fetchScalar<PgInet>()
                assertEquals(inet, value)
            }
        }

    private suspend fun decodeTest(
        inetAddress: String,
        isPrepared: Boolean,
    ) {
        val inet = PgInet.parse(inetAddress)
        val query = "SELECT '$inetAddress'::inet;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val value =
                if (isPrepared) {
                    conn.createPreparedQuery(query)
                } else {
                    conn.createQuery(query)
                }.fetchScalar<PgInet>()
            assertEquals(inet, value)
        }
    }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @CsvSource(IPV4_ADDRESS_STRING, IPV6_ADDRESS_STRING)
    fun `decode should return PgInet when simple querying postgresql inet`(
        inetAddress: String,
    ): Unit =
        runBlocking {
            decodeTest(inetAddress, isPrepared = false)
        }

    @ParameterizedTest
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    @CsvSource(IPV4_ADDRESS_STRING, IPV6_ADDRESS_STRING)
    fun `decode should return PgInet when extended querying postgresql inet`(
        inetAddress: String,
    ): Unit =
        runBlocking {
            decodeTest(inetAddress, isPrepared = true)
        }

    companion object {
        private const val IPV4_ADDRESS_STRING = "192.168.100.128/25"
        private const val IPV6_ADDRESS_STRING = "2001:4f8:3:ba:2e0:81ff:fe22:d1f1/64"
    }
}
