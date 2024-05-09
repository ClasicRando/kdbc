package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.buffer.ByteListWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import kotlin.reflect.typeOf

/**
 * Implementation of a [PgTypeDescription] for [ByteArray]. This maps to the `bytea` type in a
 * postgresql database
 */
object ByteaTypeDescription : PgTypeDescription<ByteArray>(
    pgType = PgType.Bytea,
    kType = typeOf<ByteArray>(),
) {
    /**
     * Simply writes all bytes in the [ByteArray] to the buffer.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/varlena.c#L471)
     */
    override fun encode(value: ByteArray, buffer: ByteWriteBuffer) {
        buffer.writeBytes(value)
    }

    /**
     * Reads all available bytes in the value's buffer.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/varlena.c#L490)
     */
    override fun decodeBytes(value: PgValue.Binary): ByteArray {
        return value.bytes.readBytes()
    }

    /**
     * Decode the [String] as either a prefixed hex format value (using [decodeWithPrefix]) or an
     * escape format value (using [decodeWithoutPrefix]).
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/varlena.c#L388)
     */
    override fun decodeText(value: PgValue.Text): ByteArray {
        return if (value.text.startsWith(HEX_START)) {
            decodeWithPrefix(value.text)
        } else {
            decodeWithoutPrefix(value.text)
        }
    }
}

/**
 * Implementation of a [ArrayTypeDescription] for [ByteArray]. This maps to the `bytea[]` type in a
 * postgresql database.
 */
object ByteaArrayTypeDescription : ArrayTypeDescription<ByteArray>(
    pgType = PgType.ByteaArray,
    innerType = ByteaTypeDescription,
)

/** Prefix for a hex format `bytea` value */
private const val HEX_START = "\\x"

/**
 * Convert a hex [Char] to an [Int]. Return value is always a positive [Int].
 *
 * @throws IllegalArgumentException if the hex character yields a negative digit
 */
private fun charToDigit(char: Char, index: Int): Int {
    val digit = char.digitToInt(16)
    require(digit >= 0) {
        "Illegal hexadecimal character $char at index $index"
    }
    return digit
}

/**
 * Decode the [value] into a [ByteArray], interpreting [value] as a hex formatted `bytea`.
 *
 * This reads the string 2 characters at a time, combining each pair of characters into a single
 * [Byte]. The first character of each pair is converted to an [Int] and put into the 4 left most
 * bits. The second character is converted to an [Int] and put into the 4 right most bits. Each
 * pair is then packed into a [ByteArray].
 *
 * @throws IllegalArgumentException if the number of hex characters is odd meaning we have an
 * incomplete pair
 */
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

/** Get the index or throw an [IllegalArgumentException] if the index is not valid */
private fun String.getOrThrow(index: Int): Char {
    require(index >= 0 && index < this.length) {
        "Exceeded escape sequence character. Index not found"
    }
    return this[index]
}

/**
 * Decode the [value] into a [ByteArray], interpreting [value] as an escape formatted `bytea`.
 *
 * This reads the [value] character by character, interpreting each character as a [Byte] unless
 * the character is a forward slash. In that case, it is checked if the slash is escaping a literal
 * slash, or it means that the next 3 digits need to be interpreted as a combined hexadecimal
 * [Byte] value in the format of "x{first}{second}{third}".
 */
private fun decodeWithoutPrefix(value: String): ByteArray {
    val buffer = ByteListWriteBuffer()
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
    return buffer.copyToArray()
}
