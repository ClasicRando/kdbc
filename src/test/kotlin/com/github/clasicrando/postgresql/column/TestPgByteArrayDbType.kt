package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnData
import io.ktor.utils.io.core.buildPacket
import io.ktor.utils.io.core.writeFully
import io.mockk.every
import io.mockk.impl.annotations.RelaxedMockK
import io.mockk.junit5.MockKExtension
import io.mockk.mockk
import io.mockk.slot
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.extension.ExtendWith
import java.nio.ByteBuffer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@ExtendWith(MockKExtension::class)
class TestPgByteArrayDbType {
    @RelaxedMockK
    lateinit var columnData: ColumnData

    private val ints = IntArray(256) { it }
    private val expectedByteString =
        "\\x000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F202122232425262728" +
                "292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F404142434445464748494A4B4C4D4E4" +
                "F505152535455565758595A5B5C5D5E5F606162636465666768696A6B6C6D6E6F707172737475" +
                "767778797A7B7C7D7E7F808182838485868788898A8B8C8D8E8F909192939495969798999A9B9" +
                "C9D9E9FA0A1A2A3A4A5A6A7A8A9AAABACADAEAFB0B1B2B3B4B5B6B7B8B9BABBBCBDBEBFC0C1C2" +
                "C3C4C5C6C7C8C9CACBCCCDCECFD0D1D2D3D4D5D6D7D8D9DADBDCDDDEDFE0E1E2E3E4E5E6E7E8E" +
                "9EAEBECEDEEEFF0F1F2F3F4F5F6F7F8F9FAFBFCFDFEFF"

    @Test
    fun `decode should succeed when prefixed hex value`() {
        val bytes = ints.map { it.toByte() }.toByteArray()
        val byteString = ints.joinToString(separator = "", prefix = "\\x") {
            it.toString(16).padStart(2, '0')
        }

        val result = PgByteArrayDbType.decode(columnData, byteString)

        assertTrue(result is ByteArray)
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

        val result = PgByteArrayDbType.decode(columnData, byteString)

        assertTrue(result is ByteArray)
        Assertions.assertArrayEquals(bytes, result)
    }

    @Test
    fun `encode should return hex string when byte array`() {
        val bytes = ints.map { it.toByte() }.toByteArray()

        val result = PgByteArrayDbType.encode(bytes)

        assertEquals(expectedByteString, result)
    }

    @Test
    fun `encode should return hex string when byte buffer backed by array`() {
        val byteArray = ints.map { it.toByte() }.toByteArray()
        val bytes = mockk<ByteBuffer>()
        every { bytes.hasArray() } returns true
        every { bytes.array() } returns byteArray

        val result = PgByteArrayDbType.encode(bytes)

        assertEquals(expectedByteString, result)
    }

    @Test
    fun `encode should return hex string when byte buffer not backed by array`() {
        val byteArray = ints.map { it.toByte() }.toByteArray()
        val bytes = mockk<ByteBuffer>()
        val dstSlot = slot<ByteArray>()
        every { bytes.hasArray() } returns false
        every { bytes.remaining() } returns byteArray.size
        every { bytes.get(capture(dstSlot)) } answers {
            for ((i, byte) in byteArray.withIndex()) {
                dstSlot.captured[i] = byte
            }
            bytes
        }

        val result = PgByteArrayDbType.encode(bytes)

        assertEquals(expectedByteString, result)
    }

    @Test
    fun `encode should return hex string when byte read packet`() {
        val byteArray = ints.map { it.toByte() }.toByteArray()
        val bytes = buildPacket {
            writeFully(byteArray)
        }

        val result = PgByteArrayDbType.encode(bytes)

        assertEquals(expectedByteString, result)
    }
}
