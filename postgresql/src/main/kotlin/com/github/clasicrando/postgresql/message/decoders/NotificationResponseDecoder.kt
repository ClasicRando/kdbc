package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.common.buffer.readInt
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.message.PgMessage

internal object NotificationResponseDecoder : MessageDecoder<PgMessage.NotificationResponse> {
    override fun decode(buffer: ReadBuffer): PgMessage.NotificationResponse {
        return buffer.use {
            PgMessage.NotificationResponse(
                it.readInt(),
                it.readCString(),
                it.readCString(),
            )
        }
    }
}
