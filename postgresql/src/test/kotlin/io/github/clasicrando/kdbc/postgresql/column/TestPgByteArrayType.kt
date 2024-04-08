package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.connection.use
import io.github.clasicrando.kdbc.core.result.getAs
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import kotlin.test.Test
import kotlin.test.assertEquals

class TestPgByteArrayType {
    private val fieldDescription = PgColumnDescription(
        fieldName = "",
        tableOid = 0,
        columnAttribute = 0,
        pgType = PgType.Bytea,
        dataTypeSize = 0,
        typeModifier = 0,
        formatCode = 0,
    )

    private val ints = IntArray(256) { it }

    @Test
    fun `decode should succeed when prefixed hex value`() {
        val bytes = ints.map { it.toByte() }.toByteArray()
        val byteString = ints.joinToString(separator = "", prefix = "\\x") {
            it.toString(16).padStart(2, '0')
        }

        val pgValue = PgValue.Text(byteString, fieldDescription)
        val result = byteArrayTypeDecoder.decode(pgValue)

        Assertions.assertArrayEquals(bytes, result)
    }

    @Test
    fun `decode should succeed when escaped hex value`() {
        val bytes = ints.map { it.toByte() }.toByteArray()
        val byteString = ints.joinToString(separator = "") {
            when {
                it in 32..(32 + 95) -> it.toChar().toString().replace("\\", "\\\\")
                else -> it.toString(8).padStart(3, '0').padStart(4, '\\')
            }
        }

        val pgValue = PgValue.Text(byteString, fieldDescription)
        val result = byteArrayTypeDecoder.decode(pgValue)

        Assertions.assertArrayEquals(bytes, result)
    }

    @Test
    fun `encode should accept ByteArray when querying postgresql`() = runBlocking {
        val expectedResult = byteArrayOf(0x4f, 0x5a, 0x90.toByte())
        val query = "SELECT $1 bytea_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            conn.sendPreparedStatement(query, listOf(expectedResult)).use { results ->
                assertEquals(1, results.size)
                assertEquals(1, results[0].rowsAffected)
                val rows = results[0].rows.toList()
                assertEquals(1, rows.size)
                Assertions.assertArrayEquals(
                    expectedResult,
                    rows.map { it.getAs<ByteArray>("bytea_col") }.first(),
                )
            }
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val expectedResult = byteArrayOf(0x4f, 0x5a, 0x90.toByte())
        val query = "SELECT decode('4f5a90', 'hex') bytea_col;"

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
                Assertions.assertArrayEquals(
                    expectedResult,
                    rows.map { it.getAs<ByteArray>("bytea_col") }.first(),
                )
            }
        }
    }

    @Test
    fun `decode should return bytearray when simple querying postgresql bytea`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    fun `decode should return bytearray when extended querying postgresql bytea`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }
}
