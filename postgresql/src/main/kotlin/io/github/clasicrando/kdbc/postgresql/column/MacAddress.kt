package io.github.clasicrando.kdbc.postgresql.column

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.column.ColumnDecodeError
import io.github.clasicrando.kdbc.core.column.checkOrColumnDecodeError
import io.github.clasicrando.kdbc.core.column.columnDecodeError
import io.github.clasicrando.kdbc.postgresql.type.PgMacAddress
import kotlin.reflect.typeOf

abstract class AbstractMacAddressTypeDescription(pgType: PgType) : PgTypeDescription<PgMacAddress>(
    pgType = pgType,
    kType = typeOf<PgMacAddress>(),
) {
    /**
     * Write all bytes in the [PgMacAddress] unless the supplied [pgType] is not [PgType.Macaddr8]
     * in which case the [PgMacAddress.d] and [PgMacAddress.e] are not written since they are
     * placeholder values.
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/mac.c#L140)
     */
    override fun encode(value: PgMacAddress, buffer: ByteWriteBuffer) {
        buffer.writeByte(value.a)
        buffer.writeByte(value.b)
        buffer.writeByte(value.c)
        if (pgType is PgType.Macaddr8 || pgType is PgType.Macaddr8Array) {
            buffer.writeByte(value.d)
            buffer.writeByte(value.e)
        }
        buffer.writeByte(value.f)
        buffer.writeByte(value.g)
        buffer.writeByte(value.h)
    }

    /**
     * Check the number of available bytes in the buffer to confirm it either has 6 or 8 [Byte]s.
     * If the buffer has 6 bytes then the 4th and 5th bytes that are required for a [PgMacAddress]
     * are filled in as 0xFF and 0xFE (follows the postgresql internal behaviour).
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/mac.c#L161)
     *
     * @throws ColumnDecodeError if the number of available bytes are not 6 or 8
     */
    override fun decodeBytes(value: PgValue.Binary): PgMacAddress {
        val byteCount = value.bytes.remaining
        checkOrColumnDecodeError<PgMacAddress>(
            check = byteCount == 6 || byteCount == 8,
            type = value.typeData,
        ) { "macaddr/macaddr8 values must have 6 or 8 bytes. Found $byteCount bytes" }
        return PgMacAddress(
            a = value.bytes.readByte(),
            b = value.bytes.readByte(),
            c = value.bytes.readByte(),
            d = if (byteCount == 6) 0xFF.toByte() else value.bytes.readByte(),
            e = if (byteCount == 6) 0xFE.toByte() else value.bytes.readByte(),
            f = value.bytes.readByte(),
            g = value.bytes.readByte(),
            h = value.bytes.readByte(),
        )
    }

    /**
     * Parse the provided [String] value using [PgMacAddress.fromString]
     *
     * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/mac.c#L121)
     *
     * @throws ColumnDecodeError if the string is not formatted as expected
     */
    override fun decodeText(value: PgValue.Text): PgMacAddress {
        return try {
            PgMacAddress.fromString(value.text)
        } catch (ex: Exception) {
            columnDecodeError<PgMacAddress>(
                type = value.typeData,
                reason = "Could not parse string literal to PgMacAddress value. ${ex.message}"
            )
        }
    }
}

object MacAddressTypeDescription : AbstractMacAddressTypeDescription(pgType = PgType.Macaddr)

object MacAddressArrayTypeDescription : ArrayTypeDescription<PgMacAddress>(
    pgType = PgType.MacaddrArray,
    innerType = MacAddressTypeDescription,
)

object MacAddress8TypeDescription : AbstractMacAddressTypeDescription(pgType = PgType.Macaddr8)

object MacAddress8ArrayTypeDescription : ArrayTypeDescription<PgMacAddress>(
    pgType = PgType.Macaddr8Array,
    innerType = MacAddress8TypeDescription,
)
