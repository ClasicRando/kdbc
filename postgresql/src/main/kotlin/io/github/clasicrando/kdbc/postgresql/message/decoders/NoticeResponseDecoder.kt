package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import kotlinx.io.Source

/**
 * [io.github.clasicrando.kdbc.core.message.MessageDecoder] for [PgMessage.NoticeResponse]. This
 * message is sent when the backend wants to send a non-error message to the frontend. The contents
 * of the message is a common
 * [io.github.clasicrando.kdbc.postgresql.message.information.InformationResponse] packet that is
 * using [InformationResponseDecoder.decodeToInformationResponse].
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-NOTICERESPONSE)
 */
internal object NoticeResponseDecoder : InformationResponseDecoder<PgMessage.NoticeResponse>() {
    override fun decode(buffer: Source): PgMessage.NoticeResponse {
        val fields = decodeToInformationResponse(buffer)
        return PgMessage.NoticeResponse(fields)
    }
}
