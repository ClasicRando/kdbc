package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.connection.LongBitFlags
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.Sink
import kotlinx.io.writeIntLe

/**
 * [MessageEncoder] for [MysqlMessage.SslRequest]. Precursor packet before initiating a TLS
 * handshake with the server. This message is a truncated version of the
 * [MysqlMessage.HandshakeResponse] so the [HandshakeResponseEncoder] implementation uses this
 * encoder to avoid duplicating the same encoding logic.
 *
 * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_ssl_request.html)
 */
internal object SslRequestEncoder : MessageEncoder<MysqlMessage.SslRequest, LongBitFlags> {
    private val EMPTY_19_BYTES = ByteArray(19)
    private val EMPTY_4_BYTES = ByteArray(4)

    override fun encode(value: MysqlMessage.SslRequest, buffer: Sink, context: LongBitFlags) {
        buffer.writeIntLe(context.lowInt())
        buffer.writeIntLe(value.maxPacketSize)
        buffer.writeByte(value.characterSet)

        buffer.write(EMPTY_19_BYTES)

        if (!context[Capabilities.CLIENT_MYSQL]) {
            buffer.write(EMPTY_4_BYTES)
        } else {
            buffer.writeIntLe(context.highInt())
        }
    }
}
