package com.github.clasicrando.postgresql.message

internal enum class CloseTarget(val code: Byte) {
    PreparedStatement('S'.code.toByte()),
    Portal('P'.code.toByte()),;

    companion object {
        fun fromByte(byte: Byte): CloseTarget {
            return CloseTarget.entries
                .firstOrNull { it.code == byte }
                ?: error("Cannot find close target for '${byte.toInt().toChar()}'")
        }
    }
}
