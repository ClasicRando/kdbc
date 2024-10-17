package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.postgresql.column.PgValue
import kotlin.reflect.typeOf
import kotlin.uuid.Uuid

/**
 * Implementation of a [PgTypeDescription] for the [Uuid] type. This maps to the `uuid` type in a
 * postgresql database.
 */
internal object UuidTypeDescription : PgTypeDescription<Uuid>(
    dbType = PgType.Uuid,
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
