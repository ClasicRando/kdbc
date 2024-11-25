package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.mysql.buffer.writeLengthEncoded
import io.github.clasicrando.kdbc.mysql.result.MySqlValue
import kotlinx.io.Sink
import java.util.UUID
import kotlin.reflect.typeOf
import kotlin.uuid.Uuid

internal object ByteArrayTypeDescription :
    MySqlTypeDescription<ByteArray>(dbType = MySqlType.Blob, kType = typeOf<ByteArray>()) {
    override fun isCompatible(dbType: MySqlType): Boolean {
        return dbType == MySqlType.Blob ||
            dbType == MySqlType.Varchar ||
            dbType == MySqlType.TinyBlob ||
            dbType == MySqlType.MediumBlob ||
            dbType == MySqlType.LongBlob ||
            dbType == MySqlType.String ||
            dbType == MySqlType.VarString ||
            dbType == MySqlType.Enum
    }

    override fun encode(value: ByteArray, buffer: Sink) {
        buffer.writeLengthEncoded(value)
    }

    override fun decodeBytes(value: MySqlValue.Binary): ByteArray {
        return value.bytes.readBytes()
    }

    override fun decodeText(value: MySqlValue.Text): ByteArray {
        return value.text.toByteArray()
    }
}

internal object UuidTypeDescription :
    MySqlTypeDescription<Uuid>(dbType = ByteArrayTypeDescription.dbType, kType = typeOf<Uuid>()) {
    override fun isCompatible(dbType: MySqlType): Boolean {
        return ByteArrayTypeDescription.isCompatible(dbType)
    }

    override fun encode(value: Uuid, buffer: Sink) {
        buffer.writeLengthEncoded(value.toByteArray())
    }

    override fun decodeBytes(value: MySqlValue.Binary): Uuid {
        return Uuid.fromByteArray(value.bytes.readBytes())
    }

    override fun decodeText(value: MySqlValue.Text): Uuid {
        return Uuid.parse(value.text)
    }
}

/** Implementation of a [MySqlTypeDescription] for the [java.util.UUID] type. */
internal object JUUIDTypeDescription :
    MySqlTypeDescription<UUID>(dbType = UuidTypeDescription.dbType, kType = typeOf<UUID>()) {
    /** Simply writes the bytes of the [Uuid] into the argument buffer */
    override fun encode(value: UUID, buffer: Sink) {
        buffer.writeLong(value.mostSignificantBits)
        buffer.writeLong(value.leastSignificantBits)
    }

    /** Read all bytes and pass to the [Uuid] constructor */
    override fun decodeBytes(value: MySqlValue.Binary): UUID {
        return UUID(value.bytes.readLong(), value.bytes.readLong())
    }

    /** Pass the [String] value to the [Uuid] for parsing into a [Uuid] instance */
    override fun decodeText(value: MySqlValue.Text): UUID {
        return UUID.fromString(value.text)
    }
}
