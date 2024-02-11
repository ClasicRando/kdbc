package com.github.clasicrando.postgresql.column

import io.ktor.utils.io.core.writeText
import kotlinx.io.readString

val stringTypeEncoder = PgTypeEncoder<String>(
    pgType = PgType.Text,
    compatibleTypes = arrayOf(
        PgType.Text,
        PgType.Name,
        PgType.Bpchar,
        PgType.Varchar,
        PgType.Unknown,
    )
) { value, buffer ->
    buffer.writeText(value)
}

val stringTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Text -> value.text
        is PgValue.Binary -> value.bytes.readString()
    }
}
