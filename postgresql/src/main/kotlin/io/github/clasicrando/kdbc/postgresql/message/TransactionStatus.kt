package io.github.clasicrando.kdbc.postgresql.message

private const val IDLE = 'I'.code.toByte()
private const val IN_TRANSACTION = 'T'.code.toByte()
private const val FAILED_TRANSACTION = 'E'.code.toByte()

/** Transaction status [Byte] sent by the backend when it is ready for another query */
internal enum class TransactionStatus(val code: Byte) {
    Idle(IDLE),
    InTransaction(IN_TRANSACTION),
    FailedTransaction(FAILED_TRANSACTION),
    ;

    companion object {
        fun fromByte(byte: Byte): TransactionStatus {
            return when (byte) {
                IDLE -> Idle
                IN_TRANSACTION -> InTransaction
                FAILED_TRANSACTION -> FailedTransaction
                else -> error("Cannot find transaction status for '${byte.toInt().toChar()}'")
            }
        }
    }
}
