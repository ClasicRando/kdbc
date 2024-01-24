package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.ByteReadPacket

internal class CommandCompleteDecoder(
    private val charset: Charset,
) : MessageDecoder<PgMessage.CommandComplete> {
    override fun decode(packet: ByteReadPacket): PgMessage.CommandComplete {
        val message = packet.readCString(charset = charset)
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
