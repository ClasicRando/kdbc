package io.github.clasicrando.kdbc.postgresql.type

import io.github.clasicrando.kdbc.postgresql.type.PgInet.Ipv4
import io.github.clasicrando.kdbc.postgresql.type.PgInet.Ipv6
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetAddress

/**
 * Postgresql `inet` type storing the [address] and [prefix] of a given [Ipv4] or [Ipv6] address.
 *
 * [docs](https://www.postgresql.org/docs/16/datatype-net-types.html#DATATYPE-INET)
 */
sealed class PgInet(val address: ByteArray, val prefix: UByte) {
    /**
     * IPV6 variant of a [PgInet] value where [address] is a 16 [Byte] value and [prefix] ranges
     * from 0 to 128 (default is 128).
     */
    class Ipv6(address: ByteArray, prefix: UByte) : PgInet(address, prefix) {
        init {
            require(address.size == 16) { "An Ipv4 address must be exactly 16 bytes" }
            require(prefix in 0u..128u) {
                "An Ipv4 address must have a prefix between 0 and 128"
            }
        }
    }

    /**
     * IPV4 variant of a [PgInet] value where [address] is a 4 [Byte] value and [prefix] range from
     * 0 to 32 (default is 32).
     */
    class Ipv4(address: ByteArray, prefix: UByte) : PgInet(address, prefix) {
        init {
            require(address.size == 4) { "An Ipv4 address must be exactly 4 bytes" }
            require(prefix in 0u..32u) {
                "An Ipv4 address must have a prefix between 0 and 32"
            }
        }
    }

    /** Convert the [PgInet] to its equivalent [InetAddress] type */
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
        /**
         * Parse the provided [address] into a [PgInet] type. Splits the [address] into 2 parts by
         * '/', the actual address and the prefix (can be missing). The actual address portion is
         * provided to [InetAddress.getByName] while the prefix portion is converted to a [UByte]
         * (if available). These 2 values are then passed to [fromInetAddress].
         *
         * @throws IllegalArgumentException the string contains more than 1 '/' character
         * @throws java.net.UnknownHostException if [InetAddress.getByName] fails
         */
        fun parse(address: String): PgInet {
            val parts = address.split('/')
            require(parts.size <= 2) { "Inet address must contain at most 1 '/' character" }
            val inetAddress = InetAddress.getByName(parts[0])
            val prefix = parts.getOrNull(1)?.toUByte()
            return fromInetAddress(inetAddress, prefix)
        }

        /**
         * Checks the actual type of [inetAddress] to construct the correct [Ipv4] or [Ipv6]
         * variant of [PgInet]. If the [prefix] value is null, the result will default to 32 for
         * [Ipv4] and 128 for [Ipv6].
         *
         * @throws IllegalStateException if the [inetAddress] is not [Inet4Address] or
         * [Inet6Address]
         */
        fun fromInetAddress(
            inetAddress: InetAddress,
            prefix: UByte?,
        ): PgInet {
            return when (inetAddress) {
                is Inet4Address -> Ipv4(inetAddress.address, prefix ?: 32u)
                is Inet6Address -> Ipv6(inetAddress.address, prefix ?: 128u)
                else -> error("Unexpected InetAddress variant")
            }
        }
    }
}
