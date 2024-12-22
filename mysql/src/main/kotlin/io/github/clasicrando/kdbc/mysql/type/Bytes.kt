package io.github.clasicrando.kdbc.mysql.type

import io.github.clasicrando.kdbc.mysql.buffer.writeLengthEncoded
import io.github.clasicrando.kdbc.mysql.buffer.writeLongLengthEncoded
import io.github.clasicrando.kdbc.mysql.result.MySqlValue
import java.util.UUID
import kotlin.reflect.typeOf
import kotlin.uuid.Uuid
import kotlinx.io.Sink

/**
 * Implementation of a [MySqlTypeDescription] for the [ByteArray] type. Accepts BLOB, VARCHAR,
 * TINYBLOB, MEDIUMBLOB, LONGBLOB, STRING, VARSTRING and ENUM types when decoding.
 */
internal object ByteArrayTypeDescription :
    MySqlTypeDescription<ByteArray>(dbType = MySqlType.Blob, kType = typeOf<ByteArray>()) {
    override fun isCompatible(dbType: MySqlType): Boolean {
        return dbType.inner == MySqlType.Blob.inner ||
            dbType.inner == MySqlType.Varchar.inner ||
            dbType.inner == MySqlType.TinyBlob.inner ||
            dbType.inner == MySqlType.MediumBlob.inner ||
            dbType.inner == MySqlType.LongBlob.inner ||
            dbType.inner == MySqlType.String.inner ||
            dbType.inner == MySqlType.VarString.inner ||
            dbType.inner == MySqlType.Enum.inner
    }

    /** Writes all bytes with length of the array encoded before the bytes */
    override fun encode(value: ByteArray, buffer: Sink) {
        buffer.writeLengthEncoded(value)
    }

    /** Reads all the bytes stored in the buffer */
    override fun decodeBytes(value: MySqlValue.Binary): ByteArray {
        return value.bytes.readBytes()
    }

    /** Reads all the bytes stored in the buffer without converting to a string */
    override fun decodeText(value: MySqlValue.Text): ByteArray {
        return value.bytes.readBytes()
    }
}

/**
 * Implementation of a [MySqlTypeDescription] for the [Uuid] type.
 *
 * Accepts all the same types that [ByteArrayTypeDescription] accepts.
 */
internal object UuidTypeDescription :
    MySqlTypeDescription<Uuid>(dbType = ByteArrayTypeDescription.dbType, kType = typeOf<Uuid>()) {
    override fun isCompatible(dbType: MySqlType): Boolean {
        return ByteArrayTypeDescription.isCompatible(dbType)
    }

    /** Writes the UUID bytes with the length encoded */
    override fun encode(value: Uuid, buffer: Sink) {
        buffer.writeLengthEncoded(value.toByteArray())
    }

    /** Read all bytes and pass to the [Uuid] constructor */
    override fun decodeBytes(value: MySqlValue.Binary): Uuid {
        return Uuid.fromByteArray(value.bytes.readBytes())
    }

    /** Pass the [String] value to [Uuid.parse] for parsing */
    override fun decodeText(value: MySqlValue.Text): Uuid {
        return Uuid.parse(value.text)
    }
}

/**
 * Implementation of a [MySqlTypeDescription] for the [UUID] type.
 *
 * Accepts all the same types that [ByteArrayTypeDescription] accepts.
 */
internal object JUUIDTypeDescription :
    MySqlTypeDescription<UUID>(dbType = UuidTypeDescription.dbType, kType = typeOf<UUID>()) {
    override fun isCompatible(dbType: MySqlType): Boolean {
        return UuidTypeDescription.isCompatible(dbType)
    }

    /** Writes the UUID bytes with the length of 16 bytes encoded */
    override fun encode(value: UUID, buffer: Sink) {
        buffer.writeLongLengthEncoded(16L)
        buffer.writeLong(value.mostSignificantBits)
        buffer.writeLong(value.leastSignificantBits)
    }

    /** Read all bytes and pass to the [UUID] constructor as the high and low bits */
    override fun decodeBytes(value: MySqlValue.Binary): UUID {
        return UUID(value.bytes.readLong(), value.bytes.readLong())
    }

    /** Pass the [String] value to [UUID.fromString] for parsing */
    override fun decodeText(value: MySqlValue.Text): UUID {
        return UUID.fromString(value.text)
    }
}
