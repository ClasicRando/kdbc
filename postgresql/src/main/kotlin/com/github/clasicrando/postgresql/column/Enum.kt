package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnDecodeError
import com.github.clasicrando.common.column.columnDecodeError

/**
 * Function for creating new [PgTypeEncoder] instances for enum types. This allows representing a
 * postgresql enum type as an [Enum]. The encoder writes the [Enum.name] property as text to the
 * argument buffer.
 *
 * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/enum.c#L179)
 */
inline fun <reified E : Enum<E>> enumTypeEncoder(name: String): PgTypeEncoder<E> {
    return PgTypeEncoder(PgType.ByName(name)) { value, buffer ->
        buffer.writeText(value.name)
    }
}

/**
 * Function for creating new [PgTypeDecoder] instances for enum types. This allows representing a
 * postgresql enum type as an [Enum].
 *
 * ### Binary
 * Reads all the bytes as a UTF-8 encoded [String]. Then find the enum value that matches that
 * [String] by [Enum.name]. If no match is found, throw a [ColumnDecodeError].
 *
 * ### Text
 * Use the [String] value to find the enum value that matches that [String] by [Enum.name]. If no
 * match is found, throw a [ColumnDecodeError].
 *
 * [pg source code binary](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/enum.c#L155)
 * [pg source code text](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/enum.c#L221)
 *
 * @throws ColumnDecodeError if a variant of [E] cannot be found by [Enum.name] from the decoded
 * [String] value
 */
inline fun <reified E : Enum<E>> enumTypeDecoder(): PgTypeDecoder<E> = PgTypeDecoder { value ->
    val text = when (value) {
        is PgValue.Text -> value.text
        is PgValue.Binary -> value.bytes.readText()
    }
    enumValues<E>().firstOrNull { it.name == text }
        ?: columnDecodeError<E>(value.typeData)
}
