package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.core.use
import io.github.clasicrando.kdbc.postgresql.message.PgMessage

/**
 * [MessageDecoder] for [PgMessage.NegotiateProtocolVersion]. This message is sent when the server
 * does not support the minor protocol version specified by the [PgMessage.StartupMessage] but does
 * support an earlier version of the protocol. The contents are:
 *
 * - the newest minor protocol version supported by the server for the major protocol version
 * requested as an [Int]
 * - the number of protocol options not recognized by the server
 * - [List] of the protocol option names as CStrings with a size defined by the previous value
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-NEGOTIATEPROTOCOLVERSION)
 */
internal object NegotiateProtocolVersionDecoder : MessageDecoder<PgMessage.NegotiateProtocolVersion> {
    override fun decode(buffer: ByteReadBuffer): PgMessage.NegotiateProtocolVersion {
        return buffer.use { buf ->
            val newestMinorProtocol = buf.readInt()
            val unrecognizedOptions = List(buf.readInt()) {
                buf.readCString()
            }
            PgMessage.NegotiateProtocolVersion(newestMinorProtocol, unrecognizedOptions)
        }
    }
}
