package com.github.clasicrando.postgresql.copy

internal enum class CopyFormat {
    Text,
    Binary,;

    companion object {
        private const val ONE = 1.toByte()
        private const val ZERO = 0.toByte()

        fun fromByte(byte: Byte): CopyFormat {
            return when (byte) {
                ZERO -> Text
                ONE -> Binary
                else -> error("Invalid copy format byte, must be 0 or 1")
            }
        }
    }
}
