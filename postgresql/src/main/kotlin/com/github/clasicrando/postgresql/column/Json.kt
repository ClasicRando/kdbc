package com.github.clasicrando.postgresql.column

import com.github.clasicrando.postgresql.type.PgJson

// https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/json.c#L150
val jsonTypeEncoder = PgTypeEncoder<PgJson>(
    pgType = PgType.Jsonb,
    compatibleTypes = arrayOf(PgType.Json),
) { value, buffer ->
    value.writeToBuffer(buffer)
}

val jsonTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        // https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/json.c#L136
        is PgValue.Binary -> {
            if (value.typeData.pgType == PgType.Jsonb) {
                val version = value.bytes.readByte()
                check(version == 1.toByte()) {
                    "Unsupported JSONB format version $version. Only version 1 is supported"
                }
            }
            PgJson(value.bytes)
        }
        // https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/json.c#L124
        is PgValue.Text -> PgJson(value.text)
    }
}
