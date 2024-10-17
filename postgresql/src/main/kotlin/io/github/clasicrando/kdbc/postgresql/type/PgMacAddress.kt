package io.github.clasicrando.kdbc.postgresql.type

class PgMacAddress(
    val a: Byte,
    val b: Byte,
    val c: Byte,
    val d: Byte,
    val e: Byte,
    val f: Byte,
    val g: Byte,
    val h: Byte,
) {
    var isMacAddress8: Boolean = true
        private set
    constructor(a: Byte, b: Byte, c: Byte, f: Byte, g: Byte, h: Byte)
        : this(a, b, c, DEFAULT_D, DEFAULT_E, f, g, h) {
            isMacAddress8 = false
        }

    override fun toString(): String {
        return "%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x".format(a, b, c, d, e, f, g, h)
    }

    fun toMacAddr(): PgMacAddress {
        return PgMacAddress(a, b, c, f, g, h)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (other !is PgMacAddress) {
            return false
        }

        if (a != other.a) return false
        if (b != other.b) return false
        if (c != other.c) return false
        if (d != other.d) return false
        if (e != other.e) return false
        if (f != other.f) return false
        if (g != other.g) return false
        if (h != other.h) return false

        return true
    }

    override fun hashCode(): Int {
        var result = a.toInt()
        result = 31 * result + b
        result = 31 * result + c
        result = 31 * result + d
        result = 31 * result + e
        result = 31 * result + f
        result = 31 * result + g
        result = 31 * result + h
        return result
    }

    companion object {
        private const val DEFAULT_D = 0xFF.toByte()
        private const val DEFAULT_E = 0xFE.toByte()

        fun fromString(value: String): PgMacAddress {
            val hexBytes = value
                .splitToSequence(':')
                .map { it.toByte(radix = 16) }
                .toList()
            check(hexBytes.size == 6 || hexBytes.size == 8) {
                "macaddr/macaddr8 literal value must be 6 or 8 hex numbers"
            }
            var index = 0
            return PgMacAddress(
                a = hexBytes[index++],
                b = hexBytes[index++],
                c = hexBytes[index++],
                d = if (hexBytes.size == 6) DEFAULT_D else hexBytes[index++],
                e = if (hexBytes.size == 6) DEFAULT_E else hexBytes[index++],
                f = hexBytes[index++],
                g = hexBytes[index++],
                h = hexBytes[index],
            )
        }
    }
}
