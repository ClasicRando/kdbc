package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.buffer.writeText
import com.github.clasicrando.common.column.columnDecodeError
import kotlinx.io.readString

inline fun <reified E : Enum<E>> enumTypeEncoder(name: String): PgTypeEncoder<E> {
    return PgTypeEncoder(PgType.ByName(name)) { value, buffer ->
        buffer.writeText(value.name)
    }
}

inline fun <reified E : Enum<E>> enumTypeDecoder(): PgTypeDecoder<E> = PgTypeDecoder { value ->
    val text = when (value) {
        is PgValue.Text -> value.text
        is PgValue.Binary -> value.bytes.readString()
    }
    enumValues<E>().firstOrNull { it.name == text }
        ?: columnDecodeError<E>(value.typeData)
}
