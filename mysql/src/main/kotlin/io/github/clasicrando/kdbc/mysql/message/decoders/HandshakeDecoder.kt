package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.connection.IntBitFlags
import io.github.clasicrando.kdbc.core.connection.LongBitFlags
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.authentication.AuthPlugin
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.message.Status

/**
 * [MessageDecoder] for [MysqlMessage.Handshake] packets. Provides initial details about the server,
 * it's capabilities, auth information to later negotiate connection properties.
 *
 * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html)
 */
internal object HandshakeDecoder : MessageDecoder<MysqlMessage.Handshake, Unit> {
    override fun decode(buffer: ByteReadBuffer, context: Unit): MysqlMessage.Handshake {
        val protocol = buffer.readByte()
        val serverVersion = buffer.readCString()
        val connectionId = buffer.readIntLe()
        val authPluginData1 = buffer.readBytes(8)
        buffer.readByte()

        val capabilities1 = buffer.readShortLe().toLong() and 0xff_ff
        val collation = buffer.readByte()
        val status = IntBitFlags(buffer.readShortLe())
        val capabilities2 = buffer.readShortLe().toLong() and 0xff_ff
        var capabilities = LongBitFlags((capabilities2 shl 16) or capabilities1)
        val authPluginDataLength =
            if (capabilities[Capabilities.CLIENT_PLUGIN_AUTH]) {
                buffer.readByteAsInt()
            } else {
                buffer.skip(1)
                0
            }
        buffer.skip(6)

        if (capabilities[Capabilities.CLIENT_MYSQL]) {
            buffer.skip(4)
        } else {
            val capabilities3 = buffer.readIntLe().toLong() and 0xff_ff_ff_ff
            capabilities += LongBitFlags(capabilities3 shl 32)
        }

        val authPluginData2 =
            if (capabilities[Capabilities.CLIENT_SECURE_CONNECTION]) {
                val length = (authPluginDataLength - 9).coerceAtLeast(12)
                val value = buffer.readBytes(length)
                buffer.skip(1)
                value
            } else {
                ByteArray(0)
            }

        val authPlugin =
            if (capabilities[Capabilities.CLIENT_PLUGIN_AUTH]) {
                AuthPlugin.fromName(buffer.readCString())
            } else {
                null
            }

        return MysqlMessage.Handshake(
            protocolVersion = protocol,
            serverVersion = serverVersion,
            connectionId = connectionId,
            serverCapabilities = capabilities,
            serverDefaultCollation = collation,
            status = status,
            authPlugin = authPlugin,
            authPluginData = authPluginData1.plus(authPluginData2),
        )
    }
}
