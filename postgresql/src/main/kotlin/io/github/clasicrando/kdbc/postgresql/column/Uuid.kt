package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import kotlin.uuid.Uuid
import kotlin.reflect.typeOf

/**
 * Implementation of a [PgTypeDescription] for the [Uuid] type. This maps to the `uuid` type in a
 * postgresql database.
 */
internal object UuidTypeDescription : PgTypeDescription<Uuid>(
    pgType = PgType.Uuid,
    kType = typeOf<Uuid>(),
) {
    /** Simply writes the bytes of the [Uuid] into the argument buffer */
    override fun encode(value: Uuid, buffer: ByteWriteBuffer) {
        buffer.writeBytes(value.toByteArray())
    }

    /** Read all bytes and pass to the [Uuid] constructor */
    override fun decodeBytes(value: PgValue.Binary): Uuid {
        return Uuid.fromByteArray(value.bytes.readBytes())
    }

    /** Pass the [String] value to the [Uuid] for parsing into a [Uuid] instance */
    override fun decodeText(value: PgValue.Text): Uuid {
        return Uuid.parse(value.text)
    }
}

/**
 * Implementation of an [ArrayTypeDescription] for [Uuid]. This maps to the `uuid[]` type in a
 * postgresql database.
 */
internal object UuidArrayTypeDescription : ArrayTypeDescription<Uuid>(
    pgType = PgType.UuidArray,
    innerType = UuidTypeDescription,
)
