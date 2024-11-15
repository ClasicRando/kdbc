package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.buffer.readCString
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import kotlinx.io.Source

/**
 * [MessageDecoder] for [PgMessage.NotificationResponse]. This message is sent when the frontend has
 * requested to `LISTEN` to a specific channel and the backend has a notification available. The
 * contents are:
 * - the process ID of the notifying backend as an [Int]
 * - the name of the channel that the notification is related to as a CString
 * - the "payload" of the message as a CString (can be empty)
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-NOTIFICATIONRESPONSE)
 */
internal object NotificationResponseDecoder : PgMessageDecoder<PgMessage.NotificationResponse>() {
    override fun decode(buffer: Source): PgMessage.NotificationResponse {
        return PgMessage.NotificationResponse(
            buffer.readInt(),
            buffer.readCString(),
            buffer.readCString(),
        )
    }
}
