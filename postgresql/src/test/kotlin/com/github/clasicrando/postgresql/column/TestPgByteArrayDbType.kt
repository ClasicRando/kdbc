package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.connection.use
import com.github.clasicrando.common.result.getAs
import com.github.clasicrando.postgresql.PgConnectionHelper
import com.github.clasicrando.postgresql.row.PgColumnDescription
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import kotlin.test.Test
import kotlin.test.assertEquals

class TestPgByteArrayDbType {
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

    private suspend inline fun `decode should return bytearray when querying postgresql bytea`(
        isPrepared: Boolean
    ) {
        val expectedResult = byteArrayOf(0x4f, 0x5a, 0x90.toByte())
        val query = "SELECT decode('4f5a90', 'hex');"

        val result = PgConnectionHelper.defaultConnectionWithForcedSimple().use {
            if (isPrepared) {
                it.sendPreparedStatement(query, emptyList())
            } else {
                it.sendQuery(query)
            }
        }.toList()

        assertEquals(1, result.size)
        assertEquals(1, result[0].rowsAffected)
        val rows = result[0].rows.toList()
        assertEquals(1, rows.size)
        Assertions.assertArrayEquals(expectedResult, rows.map { it.getAs<ByteArray>(0) }.first())
    }

    @Test
    fun `decode should return bytearray when simple querying postgresql bytea`(): Unit = runBlocking {
        `decode should return bytearray when querying postgresql bytea`(isPrepared = false)
    }

    @Test
    fun `decode should return bytearray when extended querying postgresql bytea`(): Unit = runBlocking {
        `decode should return bytearray when querying postgresql bytea`(isPrepared = true)
    }
}
