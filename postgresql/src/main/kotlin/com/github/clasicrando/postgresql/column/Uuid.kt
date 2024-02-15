package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.buffer.readFully
import com.github.clasicrando.common.buffer.writeFully
import kotlinx.uuid.UUID
import kotlinx.uuid.encodeToByteArray

val uuidTypeEncoder = PgTypeEncoder<UUID>(PgType.Uuid) { value, buffer ->
    buffer.writeFully(value.encodeToByteArray())
}

val uuidTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> UUID(value.bytes.readFully())
        is PgValue.Text -> UUID(value.text)
    }
}
