package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.columnDecodeError

// https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/enum.c#L179
inline fun <reified E : Enum<E>> enumTypeEncoder(name: String): PgTypeEncoder<E> {
    return PgTypeEncoder(PgType.ByName(name)) { value, buffer ->
        buffer.writeText(value.name)
    }
}

inline fun <reified E : Enum<E>> enumTypeDecoder(): PgTypeDecoder<E> = PgTypeDecoder { value ->
    val text = when (value) {
        // https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/enum.c#L155
        is PgValue.Text -> value.text
        // https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/enum.c#L221
        is PgValue.Binary -> value.bytes.readText()
    }
    enumValues<E>().firstOrNull { it.name == text }
        ?: columnDecodeError<E>(value.typeData)
}
