package com.github.kdbc.postgresql.message.decoders

import com.github.kdbc.core.buffer.ByteReadBuffer
import com.github.kdbc.core.message.MessageDecoder
import com.github.kdbc.core.use
import com.github.kdbc.postgresql.message.PgMessage

/**
 * [MessageDecoder] for [PgMessage.CommandComplete]. This message is sent after a command has been
 * successfully completed and all data that should have been transferred has been sent. The
 * contents is a CString describing the operation outcome.
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COMMANDCOMPLETE)
 */
internal object CommandCompleteDecoder : MessageDecoder<PgMessage.CommandComplete> {
    override fun decode(buffer: ByteReadBuffer): PgMessage.CommandComplete {
        val message = buffer.use { it.readCString() }
        val words = message.split(" ")
        val rowCount = when {
            words.size <= 1 -> 0
            words[0] == "INSERT" -> {
                words.drop(2).firstOrNull()?.toLongOrNull() ?: 0
            }
            else -> {
                words[1].toLongOrNull() ?: 0
            }
        }
        return PgMessage.CommandComplete(rowCount, message)
    }
}
