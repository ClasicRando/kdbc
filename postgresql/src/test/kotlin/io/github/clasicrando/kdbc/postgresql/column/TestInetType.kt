package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.result.getAs
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.PgInet
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import kotlin.test.assertEquals

class TestInetType {
    @ParameterizedTest
    @CsvSource(IPV4_ADDRESS_STRING, IPV6_ADDRESS_STRING)
    fun `encode should accept PgInet when querying postgresql`(inetAddress: String) = runBlocking {
        val inet = PgInet.parse(inetAddress)
        val query = "SELECT $1 inet_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.sendPreparedStatement(query, listOf(inet)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(inet, rows.map { it.getAs<PgInet>("inet_col") }.first())
            }
        }
    }

    private suspend fun decodeTest(inetAddress: String, isPrepared: Boolean) {
        val inet = PgInet.parse(inetAddress)
        val query = "SELECT '$inetAddress'::inet;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            if (isPrepared) {
                conn.sendPreparedStatement(query, emptyList())
            } else {
                conn.sendQuery(query)
            }.use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                assertEquals(inet, rows.map { it.getAs<PgInet>(0)!! }.first())
            }
        }
    }

    @ParameterizedTest
    @CsvSource(IPV4_ADDRESS_STRING, IPV6_ADDRESS_STRING)
    fun `decode should return PgInet when simple querying postgresql inet`(inetAddress: String): Unit = runBlocking {
        decodeTest(inetAddress, isPrepared = false)
    }

    @ParameterizedTest
    @CsvSource(IPV4_ADDRESS_STRING, IPV6_ADDRESS_STRING)
    fun `decode should return PgInet when extended querying postgresql inet`(inetAddress: String): Unit = runBlocking {
        decodeTest(inetAddress, isPrepared = true)
    }

    companion object {
        private const val IPV4_ADDRESS_STRING = "192.168.100.128/25"
        private const val IPV6_ADDRESS_STRING = "2001:4f8:3:ba:2e0:81ff:fe22:d1f1/64"
    }
}
