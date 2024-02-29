package com.github.clasicrando.postgresql.column

import kotlinx.uuid.UUID
import kotlinx.uuid.encodeToByteArray

val uuidTypeEncoder = PgTypeEncoder<UUID>(PgType.Uuid) { value, buffer ->
    buffer.writeBytes(value.encodeToByteArray())
}

val uuidTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> UUID(value.bytes.readBytes())
        is PgValue.Text -> UUID(value.text)
    }
}
