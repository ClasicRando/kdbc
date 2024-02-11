package com.github.clasicrando.postgresql.column

import com.github.clasicrando.common.column.columnDecodeError
import com.github.clasicrando.postgresql.type.PgInet
import io.ktor.utils.io.core.writeFully
import kotlinx.io.readByteArray
import java.net.Inet6Address

private const val PGSQL_AF_INET: Byte = 2
private const val PGSQL_AF_INET6: Byte = (PGSQL_AF_INET + 1).toByte()

val inetTypeEncoder = PgTypeEncoder<PgInet>(PgType.Inet) { value, buffer ->
    when (val javaInetAddress = value.toInetAddress()) {
        is Inet6Address -> {
            buffer.writeByte(PGSQL_AF_INET6)
            buffer.writeByte(value.prefix)
            buffer.writeByte(0)
            buffer.writeByte(16)
            val address = javaInetAddress.address
            check(address.size == 16) {
                "Inet address must be 16 bytes. Found ${address.size} bytes"
            }
            buffer.writeFully(address)
        }
        else -> {
            buffer.writeByte(PGSQL_AF_INET)
            buffer.writeByte(value.prefix)
            buffer.writeByte(0)
            buffer.writeByte(4)
            val address = javaInetAddress.address
            check(address.size == 4) {
                "Inet address must be 4 bytes. Found ${address.size} bytes"
            }
            buffer.writeFully(address)
        }
    }
}

val inetTypeDecoder = PgTypeDecoder { value ->
    val bytes = when (value) {
        is PgValue.Binary -> value.bytes.readByteArray()
        is PgValue.Text -> return@PgTypeDecoder PgInet.parse(value.text)
    }
    check(bytes.size >= 8) { "Inet value must be at least 8 bytes. Found ${bytes.size}" }
    val family = bytes[0]
    val prefix = bytes[1]
    val length = bytes[3]

    when {
        family == PGSQL_AF_INET && bytes.size == 8 && length == 4.toByte() -> {
            PgInet.Ipv4(bytes.sliceArray(4..<bytes.size), prefix)
        }
        family == PGSQL_AF_INET6 && bytes.size == 20 && length == 16.toByte() -> {
            PgInet.Ipv6(bytes.sliceArray(4..<bytes.size), prefix)
        }
        else -> columnDecodeError<PgInet>(value.typeData)
    }
}
