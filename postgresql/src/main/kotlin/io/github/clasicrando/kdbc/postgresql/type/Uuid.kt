package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.postgresql.column.PgValue
import kotlinx.io.Sink
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
    override fun encode(
        value: Uuid,
        buffer: Sink,
    ) {
        buffer.write(value.toByteArray())
    }

    /** Read all bytes and pass to the [Uuid] constructor */
    override fun decodeBytes(value: PgValue.Binary): Uuid =
        Uuid.fromByteArray(value.bytes.readBytes())

    /** Pass the [String] value to the [Uuid] for parsing into a [Uuid] instance */
    override fun decodeText(value: PgValue.Text): Uuid = Uuid.parse(value.text)
}

/**
 * Implementation of a [PgTypeDescription] for the [java.util.UUID] type. This maps to the `uuid`
 * type in a postgresql database.
 */
internal object JUuidTypeDescription : PgTypeDescription<java.util.UUID>(
    dbType = PgType.Uuid,
    kType = typeOf<java.util.UUID>(),
) {
    /** Simply writes the bytes of the [Uuid] into the argument buffer */
    override fun encode(
        value: java.util.UUID,
        buffer: Sink,
    ) {
        buffer.writeLong(value.mostSignificantBits)
        buffer.writeLong(value.leastSignificantBits)
    }

    /** Read all bytes and pass to the [Uuid] constructor */
    override fun decodeBytes(value: PgValue.Binary): java.util.UUID =
        java.util.UUID(value.bytes.readLong(), value.bytes.readLong())

    /** Pass the [String] value to the [Uuid] for parsing into a [Uuid] instance */
    override fun decodeText(value: PgValue.Text): java.util.UUID =
        java.util.UUID.fromString(value.text)
}
