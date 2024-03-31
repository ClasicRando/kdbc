package com.github.clasicrando.postgresql.column

import kotlinx.uuid.UUID
import kotlinx.uuid.encodeToByteArray

/**
 * Implementation of [PgTypeEncoder] for [UUID]. This maps to the `uuid` type in a postgresql
 * database. Simply writes the bytes of the [UUID] into the argument buffer.
 */
val uuidTypeEncoder = PgTypeEncoder<UUID>(PgType.Uuid) { value, buffer ->
    buffer.writeBytes(value.encodeToByteArray())
}

/**
 * Implementation of [PgTypeDecoder] for [UUID]. This maps to the `uuid` type in a postgresql
 * database.
 *
 * ### Binary
 * Read all bytes and pass to the [UUID] constructor.
 *
 * ### Text
 * Pass the [String] value to the [UUID] for parsing into a [UUID] instance.
 */
val uuidTypeDecoder = PgTypeDecoder { value ->
    when (value) {
        is PgValue.Binary -> UUID(value.bytes.readBytes())
        is PgValue.Text -> UUID(value.text)
    }
}
