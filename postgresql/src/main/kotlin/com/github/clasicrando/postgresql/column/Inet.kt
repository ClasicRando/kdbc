package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.ColumnDecodeError
import com.github.clasicrando.common.column.columnDecodeError
import com.github.clasicrando.postgresql.type.PgInet
import java.net.Inet6Address
import kotlin.reflect.typeOf

private const val PGSQL_AF_INET: Byte = 2
private const val PGSQL_AF_INET6: Byte = (PGSQL_AF_INET + 1).toByte()

/**
 * Implementation of a [PgTypeEncoder] for the [PgInet] type. This maps to the `inet` type in a
 * postgresql database. The encoder write 5 values to the buffer:
 *
 * 1. [Byte] - Header to designate value's inet type(IPV4 = [PGSQL_AF_INET] and IPV6 = [PGSQL_AF_INET6])
 * 2. [Byte] - The prefix of the address
 * 3. [Byte] - Is CIDR flag, always 0
 * 4. [Byte] - The number of following bytes (IPV4 = 4, IPV6 = 16)
 * 5. [ByteArray] - bytes that represent the address
 *
 * [pg source code](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/network.c#L250)
 *
 * @throws IllegalStateException if the address does not contain the right number of bytes
 */
val inetTypeEncoder = PgTypeEncoder<PgInet>(
    pgType = PgType.Inet,
    encodeTypes = listOf(typeOf<PgInet.Ipv4>(), typeOf<PgInet.Ipv6>()),
) { value, buffer ->
    when (val javaInetAddress = value.toInetAddress()) {
        is Inet6Address -> {
            buffer.writeByte(PGSQL_AF_INET6)
            buffer.writeByte(value.prefix.toByte())
            buffer.writeByte(0)
            buffer.writeByte(16)
            val address = javaInetAddress.address
            check(address.size == 16) {
                "Inet address must be 16 bytes. Found ${address.size} bytes"
            }
            buffer.writeBytes(address)
        }
        else -> {
            buffer.writeByte(PGSQL_AF_INET)
            buffer.writeByte(value.prefix.toByte())
            buffer.writeByte(0)
            buffer.writeByte(4)
            val address = javaInetAddress.address
            check(address.size == 4) {
                "Inet address must be 4 bytes. Found ${address.size} bytes"
            }
            buffer.writeBytes(address)
        }
    }
}

/**
 * Implementation of a [PgTypeDecoder] for the [PgInet] type. This maps to the `inet` type in a
 * postgresql database.
 *
 * ### Binary
 * Reads the buffer for the following components:
 *
 * 1. [Byte] - Header to designate value's inet type(IPV4 = [PGSQL_AF_INET] and IPV6 = [PGSQL_AF_INET6])
 * 2. [Byte] - The prefix of the address
 * 3. [Byte] - Is CIDR flag, always 0
 * 4. [Byte] - The number of following bytes (IPV4 = 4, IPV6 = 16)
 * 5. [ByteArray] - bytes that represent the address
 *
 * For IPV4 addresses the number of bytes in the address array and the length must be 4. For IPV6
 * addresses the number of bytes in the address array and length must be 16. With the address array
 * and prefix, the appropriate [PgInet] instance is created.
 *
 * ### Text
 * Attempt to parse the [String] into a [PgInet] using the [PgInet.parse] method.
 *
 * [pg source code binary](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/network.c#L292)
 * [pg source code text](https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/network.c#L165)
 *
 * @throws ColumnDecodeError if the text value cannot be parsed into a [PgInet] or the binary
 * values cannot be used to construct a [PgInet]
 */
val inetTypeDecoder = PgTypeDecoder { value ->
    val bytes = when (value) {
        is PgValue.Binary -> value.bytes
        is PgValue.Text -> return@PgTypeDecoder try {
            PgInet.parse(value.text)
        } catch (ex: Throwable) {
            columnDecodeError<PgInet>(
                type = value.typeData,
                reason = "Cannot parse a PgInet from '${value.text}'"
            )
        }
    }
    check(bytes.remaining >= 8) {
        "Inet value must be at least 8 bytes. Found ${bytes.remaining}"
    }
    val family = bytes.readByte()
    val prefix = bytes.readByte().toUByte()
    bytes.readByte()
    val length = bytes.readByte()

    val addressBytes = bytes.readBytes()
    when {
        family == PGSQL_AF_INET && addressBytes.size == 4 && length == 4.toByte() -> {
            PgInet.Ipv4(addressBytes, prefix)
        }
        family == PGSQL_AF_INET6 && addressBytes.size == 16 && length == 16.toByte() -> {
            PgInet.Ipv6(addressBytes, prefix)
        }
        else -> columnDecodeError<PgInet>(value.typeData)
    }
}
