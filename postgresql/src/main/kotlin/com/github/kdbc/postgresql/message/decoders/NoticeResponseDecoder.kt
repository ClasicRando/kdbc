package com.github.kdbc.postgresql.message.decoders

import com.github.kdbc.core.buffer.ByteReadBuffer
import com.github.kdbc.core.message.MessageDecoder
import com.github.kdbc.postgresql.message.PgMessage
import com.github.kdbc.postgresql.message.information.InformationResponse

/**
 * [MessageDecoder] for [PgMessage.NoticeResponse]. This message is sent when the backend wants to
 * send a non-error message to the frontend. The contents of the message is a common
 * [InformationResponse] packet that is using
 * [InformationResponseDecoder.decodeToInformationResponse].
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-NOTICERESPONSE)
 */
internal object NoticeResponseDecoder : InformationResponseDecoder<PgMessage.NoticeResponse>() {
    override fun decode(buffer: ByteReadBuffer): PgMessage.NoticeResponse {
        val fields = decodeToInformationResponse(buffer)
        return PgMessage.NoticeResponse(fields)
    }
}
