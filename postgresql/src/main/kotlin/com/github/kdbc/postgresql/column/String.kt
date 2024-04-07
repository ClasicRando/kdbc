package com.github.kdbc.postgresql.column

/**
 * Implementation of [PgTypeEncoder] for [String]. This maps to the `text`/`name`/`bpchar`/`varchar`
 * types in a postgresql database. Simply writes the [String] value to the buffer in UTF8 encoding.
 */
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

/**
 * Implementation of [PgTypeDecoder] for [String]. This maps to the `text`/`name`/`bpchar`/`varchar`
 * types in a postgresql database
 *
 * ### Binary
 * Read the bytes as text using UFT8 encoding.
 *
 * ### Text
 * Return the [PgValue.Text.text] value directly
 */
val stringTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Text -> value.text
        is PgValue.Binary -> value.bytes.readText()
    }
}
