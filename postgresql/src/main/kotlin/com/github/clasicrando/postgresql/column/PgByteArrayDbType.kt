package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnData
import com.github.clasicrando.common.column.DbType
import com.github.clasicrando.common.column.columnEncodeError
import io.ktor.utils.io.core.ByteReadPacket
import io.ktor.utils.io.core.buildPacket
import io.ktor.utils.io.core.readBytes
import java.nio.ByteBuffer
import kotlin.reflect.KClass

object PgByteArrayDbType : DbType {
    private const val HEX_START = "\\x"
    private val hexStartChars = HEX_START.toCharArray()
    private val digits = charArrayOf(
        '0',
        '1',
        '2',
        '3',
        '4',
        '5',
        '6',
        '7',
        '8',
        '9',
        'A',
        'B',
        'C',
        'D',
        'E',
        'F',
    )

    override fun decode(type: ColumnData, value: String): Any {
        return if (value.startsWith(HEX_START)) {
            decodeWithPrefix(value)
        } else {
            decodeWithoutPrefix(value)
        }
    }

    private fun charToDigit(char: Char, index: Int): Int {
        val digit = char.digitToInt(16)
        require(digit >= 0) {
            "Illegal hexadecimal character $char at index $index"
        }
        return digit
    }

    private fun decodeWithPrefix(value: String): ByteArray {
        val size = value.length - HEX_START.length

        require(size.and(0x01) == 0) {
            "Hex encoded byte array must have an even number of elements"
        }

        var index = HEX_START.length
        // Size of result array is size / 2 or right shift of 1
        return ByteArray(size.shr(1)) {
            var currentByte = charToDigit(value[index], index).shl(4)
            index++
            val other = charToDigit(value[index], index)
            currentByte = currentByte.or(other)
            index++
            currentByte.and(0xFF).toByte()
        }
    }

    private fun String.getOrThrow(index: Int): Char {
        return this.getOrNull(index)
            ?: error("Exceeded escape sequence character, nothing found")
    }

    private fun decodeWithoutPrefix(value: String): ByteArray {
        return buildPacket {
            val maxIndex = value.length - 1
            var index = 0

            while (index <= maxIndex) {
                val currentChar = value[index]
                index++

                if (currentChar != '\\') {
                    writeByte(currentChar.code.toByte())
                    continue
                }

                val nextChar = value.getOrThrow(index)
                index++

                if (nextChar == '\\') {
                    writeByte('\\'.code.toByte())
                    continue
                }

                val secondDigit = value.getOrThrow(index)
                index++
                val thirdDigit = value.getOrThrow(index)
                index++
                writeByte("0$nextChar$secondDigit$thirdDigit".toInt(8).toByte())
            }
        }.readBytes()
    }

    override val encodeType: KClass<*> = ByteArray::class

    override fun encode(value: Any): String {
        val bytes = when {
            value is ByteArray -> value
            value is ByteBuffer && value.hasArray() -> value.array()
            value is ByteBuffer -> {
                val array = ByteArray(value.remaining())
                value.get(array)
                array
            }
            value is ByteReadPacket -> value.readBytes()
            else -> columnEncodeError<ByteArray>(value)
        }

        val size = bytes.size * 2 + hexStartChars.size
        var charIndex = 0
        val chars = CharArray(size)
        for (char in hexStartChars) {
            chars[charIndex] = char
            charIndex++
        }

        for (byte in bytes) {
            val byteAsInt = byte.toInt()
            chars[charIndex] = digits[(0xF0 and byteAsInt) ushr 4]
            charIndex++

            chars[charIndex] = digits[0x0F and byteAsInt]
            charIndex++
        }

        return String(chars)
    }
}
