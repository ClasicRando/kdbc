package com.github.clasicrando.postgresql.column

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
        is PgValue.Binary -> value.bytes.readText()
    }
}
