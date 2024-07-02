package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import kotlinx.uuid.UUID
import kotlinx.uuid.encodeToByteArray
import kotlin.reflect.typeOf

/**
 * Implementation of a [PgTypeDescription] for the [UUID] type. This maps to the `uuid` type in a
 * postgresql database.
 */
internal object UuidTypeDescription : PgTypeDescription<UUID>(
    pgType = PgType.Uuid,
    kType = typeOf<UUID>(),
) {
    /** Simply writes the bytes of the [UUID] into the argument buffer */
    override fun encode(value: UUID, buffer: ByteWriteBuffer) {
        buffer.writeBytes(value.encodeToByteArray())
    }

    /** Read all bytes and pass to the [UUID] constructor */
    override fun decodeBytes(value: PgValue.Binary): UUID {
        return UUID(value.bytes.readBytes())
    }

    /** Pass the [String] value to the [UUID] for parsing into a [UUID] instance */
    override fun decodeText(value: PgValue.Text): UUID {
        return UUID(value.text)
    }
}

/**
 * Implementation of an [ArrayTypeDescription] for [UUID]. This maps to the `uuid[]` type in a
 * postgresql database.
 */
internal object UuidArrayTypeDescription : ArrayTypeDescription<UUID>(
    pgType = PgType.UuidArray,
    innerType = UuidTypeDescription,
)
