package io.github.clasicrando.kdbc.postgresql.copy

private const val ONE = 1.toByte()
private const val ZERO = 0.toByte()

/**
 * Postgresql copy format types. Currently only Text & CSV format are supported since binary
 * formatting is quite a bit harder and more volatile to change. The text based formats are still
 * very fast so needing the binary format is not likely.
 */
enum class CopyFormat(val formatCode: Byte) {
    Text(ZERO),
    CSV(ZERO),
    Binary(ONE),
    ;

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

        fun fromByte(byte: Byte): CopyFormat {
            return when (byte) {
                ZERO -> Text
                ONE -> Binary
                else -> error("Invalid copy format byte, must be 0 or 1")
            }
        }
    }
}
