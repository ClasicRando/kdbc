package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.connection.LongBitFlags
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

/**
 * [MessageEncoder] for [MysqlMessage.SslRequest]. Precursor packet before initiating a TLS
 * handshake with the server. This message is a truncated version of the
 * [MysqlMessage.HandshakeResponse] so the [HandshakeResponseEncoder] implementation uses this
 * encoder to avoid duplicating the same encoding logic.
 *
 * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_ssl_request.html)
 */
internal object SslRequestEncoder : MessageEncoder<MysqlMessage.SslRequest, LongBitFlags> {
    override fun encode(
        value: MysqlMessage.SslRequest,
        buffer: ByteWriteBuffer,
        context: LongBitFlags,
    ) {
        buffer.writeIntLe(context.lowInt())
        buffer.writeIntLe(value.maxPacketSize)
        buffer.writeByte(value.characterSet)

        buffer.writeEmpty(19)

        if (!context[Capabilities.CLIENT_MYSQL]) {
            buffer.writeEmpty(4)
        } else {
            buffer.writeIntLe(context.highInt())
        }
    }
}
