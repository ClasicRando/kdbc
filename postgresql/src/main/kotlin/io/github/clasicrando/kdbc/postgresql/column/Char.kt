package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import kotlin.reflect.typeOf

/**
 * Implementation of a [PgTypeDescription] for the [Byte] type. This maps to the `"char"` type in a
 * postgresql database.
 */
object CharTypeDescription : PgTypeDescription<Byte>(
    pgType = PgType.Char,
    kType = typeOf<Char>(),
) {
    /** Simply write the [Byte] value to the buffer */
    override fun encode(value: Byte, buffer: ByteWriteBuffer) {
        buffer.writeByte(value)
    }

    /**
     * Reads the first byte from the value buffer provided. If not bytes are remaining, returns 0
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/char.c#L105)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the text representation
     * is too long
     */
    override fun decodeBytes(value: PgValue.Binary): Byte {
        return if (value.bytes.remaining() > 0) value.bytes.readByte() else 0
    }

    /**
     * Converts the text into a [Byte] depending on the [String.length]:
     * - when 4, the [Byte] has been packed into 3 characters with a forward slash prefix to
     * accommodate non-ascii char values, see pg code for more details explanation
     * - when 1, the [Byte] is just the ascii representation of the character
     * - when 0, return 0
     * - otherwise, thrown a [io.github.clasicrando.kdbc.core.column.ColumnDecodeError] is thrown
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/char.c#L64)
     *
     * @throws io.github.clasicrando.kdbc.core.column.ColumnDecodeError if the text representation
     * is too long
     */
    override fun decodeText(value: PgValue.Text): Byte {
        return when (value.text.length) {
            4 -> {
                val first = value.text[1].code shl 6
                val second = value.text[2].code shl 3
                val third = value.text[3].code
                (first or second or third).toByte()
            }
            1 -> value.text[0].code.toByte()
            0 -> 0.toByte()
            else -> columnDecodeError<Byte>(
                type = value.typeData,
                reason = "Received invalid \"char\" text, '${value.text}'",
            )
        }
    }
}

/**
 * Implementation of an [ArrayTypeDescription] for [Byte]. This maps to the `"char"[]` type in a
 * postgresql database.
 */
object CharArrayTypeDescription : ArrayTypeDescription<Byte>(
    pgType = PgType.CharArray,
    innerType = CharTypeDescription,
)
