package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.message.PgMessage

internal object CommandCompleteDecoder : MessageDecoder<PgMessage.CommandComplete> {
    override fun decode(buffer: ReadBuffer): PgMessage.CommandComplete {
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
