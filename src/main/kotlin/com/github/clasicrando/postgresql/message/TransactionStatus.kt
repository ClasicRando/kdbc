package com.github.clasicrando.postgresql.message

enum class TransactionStatus(val code: Byte) {
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
