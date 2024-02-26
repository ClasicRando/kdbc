package com.github.clasicrando.postgresql.type

import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetAddress

sealed class PgInet(val address: ByteArray, val prefix: UByte) {
    class Ipv6(address: ByteArray, prefix: UByte) : PgInet(address, prefix) {
        init {
            require(address.size == 16) { "An Ipv4 address must be exactly 16 bytes" }
            require(prefix in 0u..128u) {
                "An Ipv4 address must have a prefix between 0 and 128"
            }
        }
    }
    class Ipv4(address: ByteArray, prefix: UByte) : PgInet(address, prefix) {
        init {
            require(address.size == 4) { "An Ipv4 address must be exactly 4 bytes" }
            require(prefix in 0u..32u) {
                "An Ipv4 address must have a prefix between 0 and 32"
            }
        }
    }

    fun toInetAddress(): InetAddress {
        return InetAddress.getByAddress(address)
    }

    override fun toString(): String {
        return "PgInet(address=${address.joinToString(prefix = "[", postfix = "]")},prefix=$prefix)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is PgInet) return false
        if (!address.contentEquals(other.address)) return false
        if (prefix != other.prefix) return false

        return true
    }

    override fun hashCode(): Int {
        var result = address.contentHashCode()
        result = 31 * result + prefix.toInt()
        return result
    }

    companion object {
        // https://github.com/postgres/postgres/blob/874d817baa160ca7e68bee6ccc9fc1848c56e750/src/backend/utils/adt/network.c#L165
        fun parse(address: String): PgInet {
            val parts = address.split('/')
            require(parts.size <= 2) { "Inet address must contain at most 1 '/' character" }
            val inetAddress = InetAddress.getByName(parts[0])
            val prefix = parts.getOrNull(1)?.toUByte()
            return fromInetAddress(inetAddress, prefix)
        }

        fun fromInetAddress(inetAddress: InetAddress, prefix: UByte?): PgInet {
            return when(inetAddress) {
                is Inet4Address -> Ipv4(inetAddress.address, prefix ?: 32u)
                is Inet6Address -> Ipv6(inetAddress.address, prefix ?: 128u)
                else -> error("Unexpected InetAddress variant")
            }
        }
    }
}
