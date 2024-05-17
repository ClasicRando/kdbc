package io.github.clasicrando.kdbc.postgresql.type

data class PgMacAddress(
    val a: Byte,
    val b: Byte,
    val c: Byte,
    val d: Byte,
    val e: Byte,
    val f: Byte,
    val g: Byte,
    val h: Byte,
) {
    override fun toString(): String {
        return "%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x".format(a, b, c, d, e, f, g, h)
    }

    fun toMacAddr(): PgMacAddress {
        return copy(d = DEFAULT_D, e = DEFAULT_E)
    }

    companion object {
        const val DEFAULT_D = 0xFF.toByte()
        const val DEFAULT_E = 0xFE.toByte()

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
