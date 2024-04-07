package com.github.kdbc.postgresql.message

/** Specified target of [PgMessage.Close] and [PgMessage.Describe] */
internal enum class MessageTarget(val code: Byte) {
    PreparedStatement('S'.code.toByte()),
    Portal('P'.code.toByte()),
}
