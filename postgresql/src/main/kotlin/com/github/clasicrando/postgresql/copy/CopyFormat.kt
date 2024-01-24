package com.github.clasicrando.postgresql.copy

enum class CopyFormat {
    Text,
    CSV,
    Binary,;

    override fun toString(): String {
        return when (this) {
            Text -> TEXT_NAME
            CSV -> CSV_NAME
            Binary -> BINARY_NAME
        }
    }

    companion object {
        private const val TEXT_NAME = "text"
        private const val CSV_NAME = "csv"
        private const val BINARY_NAME = "binary"
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
