package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeCString
import io.github.clasicrando.kdbc.core.connection.LongBitFlags
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.buffer.writeLengthEncoded
import io.github.clasicrando.kdbc.mysql.buffer.writeLongLengthEncoded
import io.github.clasicrando.kdbc.mysql.buffer.writeStringLengthEncoded
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

/**
 * [MessageEncoder] for [MysqlMessage.HandshakeResponse]. Sends the post TLS handshake (if TLS is
 * available and selected) response to the server to response to an authentication requirement or
 * start the auth flow.
 *
 * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_response.html#sect_protocol_connection_phase_packets_protocol_handshake_response41)
 */
internal object HandshakeResponseEncoder :
    MessageEncoder<MysqlMessage.HandshakeResponse, LongBitFlags> {
    override fun encode(
        value: MysqlMessage.HandshakeResponse,
        buffer: ByteWriteBuffer,
        context: LongBitFlags,
    ) {
        var capabilities = context
        if (value.authPlugin == null) {
            capabilities -= Capabilities.CLIENT_PLUGIN_AUTH
        }

        val sslRequest = MysqlMessage.SslRequest(value.maxPacketSize, value.characterSet)
        SslRequestEncoder.encode(sslRequest, buffer, capabilities)

        buffer.writeCString(value.username)

        if (capabilities[Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA]) {
            buffer.writeLengthEncoded(value.authResponse ?: ByteArray(0))
        } else if (capabilities[Capabilities.CLIENT_SECURE_CONNECTION]) {
            buffer.writeByte(value.authResponse?.size?.toByte() ?: 0)
            buffer.writeBytes(value.authResponse ?: ByteArray(0))
        } else {
            buffer.writeByte(0)
        }

        if (capabilities[Capabilities.CLIENT_CONNECT_WITH_DB]) {
            if (value.database != null) {
                buffer.writeCString(value.database)
            } else {
                buffer.writeByte(0)
            }
        }

        if (capabilities[Capabilities.CLIENT_PLUGIN_AUTH]) {
            if (value.authPlugin != null) {
                buffer.writeCString(value.authPlugin.name)
            } else {
                buffer.writeByte(0)
            }
        }

        if (capabilities[Capabilities.CLIENT_CONNECT_ATTRS]) {
            buffer.writeLongLengthEncoded(value.sessionProperties.size.toLong())
            for ((key, value) in value.sessionProperties) {
                buffer.writeStringLengthEncoded(key)
                buffer.writeStringLengthEncoded(value)
            }
        }
    }
}
