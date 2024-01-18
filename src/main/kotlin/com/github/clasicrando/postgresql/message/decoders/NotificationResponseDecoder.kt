package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.ByteReadPacket
import io.ktor.utils.io.core.readInt

internal class NotificationResponseDecoder(
    private val charset: Charset,
) : MessageDecoder<PgMessage.NotificationResponse> {
    override fun decode(packet: ByteReadPacket): PgMessage.NotificationResponse {
        return PgMessage.NotificationResponse(
            packet.readInt(),
            packet.readCString(charset),
            packet.readCString(charset),
        )
    }
}
