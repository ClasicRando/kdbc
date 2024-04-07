package com.github.kdbc.postgresql.column

import com.github.kdbc.core.column.ColumnDecodeError
import com.github.kdbc.core.column.columnDecodeError

/**
 * Implementation of a [PgTypeEncoder] for the [Byte] type. This maps to the `"char"` type in a
 * postgresql database. The encoder simply writes the [Byte] value to the argument buffer.
 */
val charTypeEncoder = PgTypeEncoder<Byte>(PgType.Char) { value, buffer ->
    buffer.writeByte(value)
}

/**
 * Implementation of a [PgTypeDecoder] for the [Byte] type. This maps to the `"char"` type in a
 * postgresql database.
 *
 * ### Binary
 * Reads the first byte from the value buffer provided. If not bytes are remaining, returns 0
 *
 * ### Text
 * Converts the text into a [Byte] depending on the [String.length]:
 * - when 4, the [Byte] has been packed into 3 characters with a forward slash prefix to
 * accommodate non-ascii char values, see pg code for more details explanation
 * - when 1, the [Byte] is just the ascii representation of the character
 * - when 0, return 0
 * - otherwise, thrown a [ColumnDecodeError] is thrown
 *
 * [pg source code binary](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/char.c#L105)
 * [pg source code text](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/char.c#L64)
 *
 * @throws ColumnDecodeError if the text representation is too long
 */
val charTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> if (value.bytes.remaining > 0) value.bytes.readByte() else 0
        is PgValue.Text -> {
            when (value.text.length) {
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
}
