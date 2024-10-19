package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.DEFAULT_KDBC_TEST_TIMEOUT
import io.github.clasicrando.kdbc.core.query.bind
import io.github.clasicrando.kdbc.core.query.fetchScalar
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.PgConnectionHelper
import io.github.clasicrando.kdbc.postgresql.type.ByteaTypeDescription
import io.github.clasicrando.kdbc.postgresql.type.PgType
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Timeout
import kotlin.test.Test

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
        val result = ByteaTypeDescription.decode(pgValue)

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
        val result = ByteaTypeDescription.decode(pgValue)

        Assertions.assertArrayEquals(bytes, result)
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `encode should accept ByteArray when querying postgresql`(): Unit = runBlocking {
        val expectedResult = byteArrayOf(0x4f, 0x5a, 0x90.toByte())
        val query = "SELECT $1 bytea_col;"

        PgConnectionHelper.defaultConnection().use { conn ->
            val value = conn.createPreparedQuery(query)
                .bind(expectedResult)
                .fetchScalar<ByteArray>()
            Assertions.assertArrayEquals(expectedResult, value)
        }
    }

    private suspend fun decodeTest(isPrepared: Boolean) {
        val expectedResult = byteArrayOf(0x4f, 0x5a, 0x90.toByte())
        val query = "SELECT decode('4f5a90', 'hex') bytea_col;"

        PgConnectionHelper.defaultConnectionWithForcedSimple().use { conn ->
            val value = if (isPrepared) {
                conn.createPreparedQuery(query)
            } else {
                conn.createQuery(query)
            }.fetchScalar<ByteArray>()
            Assertions.assertArrayEquals(expectedResult, value)
        }
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return bytearray when simple querying postgresql bytea`(): Unit = runBlocking {
        decodeTest(isPrepared = false)
    }

    @Test
    @Timeout(value = DEFAULT_KDBC_TEST_TIMEOUT)
    fun `decode should return bytearray when extended querying postgresql bytea`(): Unit = runBlocking {
        decodeTest(isPrepared = true)
    }
}
