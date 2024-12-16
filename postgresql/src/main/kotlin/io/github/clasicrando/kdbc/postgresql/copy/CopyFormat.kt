package io.github.clasicrando.kdbc.postgresql.copy

import io.github.clasicrando.kdbc.postgresql.column.PgFormatCode

/**
 * Postgresql copy format types. Currently only Text & CSV format are supported since binary
 * formatting is quite a bit harder and more volatile to change. The text based formats are still
 * very fast so needing the binary format is not likely.
 */
public enum class CopyFormat(public val formatCode: PgFormatCode) {
    Text(PgFormatCode.Text),
    CSV(PgFormatCode.Text),
    Binary(PgFormatCode.Binary);

    override fun toString(): String {
        return when (this) {
            Text -> TEXT_NAME
            CSV -> CSV_NAME
            Binary -> BINARY_NAME
        }
    }

    internal companion object {
        private const val TEXT_NAME = "text"
        private const val CSV_NAME = "csv"
        private const val BINARY_NAME = "binary"

        fun fromByte(byte: Byte): CopyFormat {
            return when (byte) {
                0.toByte() -> Text
                1.toByte() -> Binary
                else -> error("Invalid copy format byte $byte, must be 0 or 1")
            }
        }
    }
}
