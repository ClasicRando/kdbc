package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.readCString
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import kotlinx.io.Buffer

/**
 * [MessageDecoder] for [PgMessage.CommandComplete]. This message is sent after a command has been
 * successfully completed and all data that should have been transferred has been sent. The contents
 * is a CString describing the operation outcome.
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COMMANDCOMPLETE)
 */
internal object CommandCompleteDecoder : PgMessageDecoder<PgMessage.CommandComplete>() {
    override fun decode(buffer: Buffer): PgMessage.CommandComplete {
        val message = buffer.readCString()
        val words = message.split(" ")
        val rowCount =
            when {
                words.size <= 1 -> 0
                words[0] == "INSERT" -> words.getOrNull(2)?.toLongOrNull() ?: 0
                else -> words[1].toLongOrNull() ?: 0
            }
        return PgMessage.CommandComplete(rowCount, message)
    }
}
