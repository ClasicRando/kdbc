package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.message.information.InformationResponse

/**
 * [MessageDecoder] for [PgMessage.ErrorResponse]. This message is sent when the backend encounters
 * an error either in its internal process or as a result of a message passed from the frontend.
 * The contents of the message is a common [InformationResponse] packet that is using
 * [InformationResponseDecoder.decodeToInformationResponse].
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-ERRORRESPONSE)
 */
internal object ErrorResponseDecoder : InformationResponseDecoder<PgMessage.ErrorResponse>() {
    override fun decode(buffer: ByteReadBuffer): PgMessage.ErrorResponse {
        val informationResponse = decodeToInformationResponse(buffer)
        return PgMessage.ErrorResponse(informationResponse)
    }
}
