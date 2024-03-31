package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.message.PgMessage

/**
 * [MessageDecoder] for [PgMessage.NotificationResponse]. This message is sent when the frontend
 * has requested to `LISTEN` to a specific channel and the backend has a notification available.
 * The contents are:
 *
 * - the process ID of the notifying backend as an [Int]
 * - the name of the channel that the notification is related to as a CString
 * - the "payload" of the message as a CString (can be empty)
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-NOTIFICATIONRESPONSE)
 */
internal object NotificationResponseDecoder : MessageDecoder<PgMessage.NotificationResponse> {
    override fun decode(buffer: ByteReadBuffer): PgMessage.NotificationResponse {
        return buffer.use {
            PgMessage.NotificationResponse(
                it.readInt(),
                it.readCString(),
                it.readCString(),
            )
        }
    }
}
