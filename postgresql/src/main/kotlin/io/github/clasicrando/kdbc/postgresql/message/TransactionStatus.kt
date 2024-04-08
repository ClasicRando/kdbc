package io.github.clasicrando.kdbc.postgresql.message

/** Transaction status [Byte] sent by the backend when it is ready for another query */
internal enum class TransactionStatus(val code: Byte) {
    Idle('I'.code.toByte()),
    InTransaction('T'.code.toByte()),
    FailedTransaction('E'.code.toByte()),;

    companion object {
        fun fromByte(byte: Byte): TransactionStatus {
            return TransactionStatus.entries
                .firstOrNull { it.code == byte }
                ?: error("Cannot find transaction status for '${byte.toInt().toChar()}'")
        }
    }
}
