package com.github.clasicrando.postgresql.column

import com.github.clasicrando.postgresql.row.PgColumnDescription
import io.mockk.junit5.MockKExtension
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.extension.ExtendWith
import kotlin.test.Test

@ExtendWith(MockKExtension::class)
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
}
