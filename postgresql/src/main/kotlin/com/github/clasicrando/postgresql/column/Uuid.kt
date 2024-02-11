package com.github.clasicrando.postgresql.column

import io.ktor.utils.io.core.writeFully
import kotlinx.io.readByteArray
import kotlinx.uuid.UUID
import kotlinx.uuid.encodeToByteArray

val uuidTypeEncoder = PgTypeEncoder<UUID>(PgType.Uuid) { value, buffer ->
    buffer.writeFully(value.encodeToByteArray())
}

val uuidTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> UUID(value.bytes.readByteArray())
        is PgValue.Text -> UUID(value.text)
    }
}
