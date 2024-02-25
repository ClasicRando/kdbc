package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.buffer.ByteWriteBuffer

val byteArrayTypeEncoder = PgTypeEncoder<ByteArray>(PgType.Bytea) { value, buffer ->
    buffer.writeFully(value)
}

val byteArrayTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> value.bytes.readBytes()
        is PgValue.Text -> {
            if (value.text.startsWith(HEX_START)) {
                decodeWithPrefix(value.text)
            } else {
                decodeWithoutPrefix(value.text)
            }
        }
    }
}

private const val HEX_START = "\\x"

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
    val buffer = ByteWriteBuffer()
    val maxIndex = value.length - 1
    var index = 0

    while (index <= maxIndex) {
        val currentChar = value[index]
        index++

        if (currentChar != '\\') {
            buffer.writeByte(currentChar.code.toByte())
            continue
        }

        val nextChar = value.getOrThrow(index)
        index++

        if (nextChar == '\\') {
            buffer.writeByte('\\'.code.toByte())
            continue
        }

        val secondDigit = value.getOrThrow(index)
        index++
        val thirdDigit = value.getOrThrow(index)
        index++
        buffer.writeByte("0$nextChar$secondDigit$thirdDigit".toInt(8).toByte())
    }
    return buffer.writeToArray()
}
