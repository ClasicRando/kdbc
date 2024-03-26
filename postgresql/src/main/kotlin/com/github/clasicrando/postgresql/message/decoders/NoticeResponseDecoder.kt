package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.message.information.InformationResponse

/**
 * [MessageDecoder] for [PgMessage.BackendKeyData]. This message is sent when the backend
 * wants to send a non-error message to the frontend. The contents of the message is a common
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
