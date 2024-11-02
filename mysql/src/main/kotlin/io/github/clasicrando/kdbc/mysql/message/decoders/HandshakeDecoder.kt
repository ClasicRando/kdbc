package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.authentication.AuthPlugin
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.message.Status

internal object HandshakeDecoder : MessageDecoder<MysqlMessage.Handshake, Unit> {
    override fun decode(
        buffer: ByteReadBuffer,
        context: Unit,
    ): MysqlMessage.Handshake {
        val protocol = buffer.readByte()
        val serverVersion = buffer.readCString()
        val connectionId = buffer.readIntLe()
        val authPluginData1 = buffer.readBytes(8)
        buffer.readByte()

        val capabilities1 = buffer.readShortLe().toLong()
        val collation = buffer.readByte()
        val status = Status(buffer.readShortLe())
        val capabilities2 = buffer.readShortLe().toLong()
        var capabilities = Capabilities((capabilities2 shl 16) or capabilities1)
        val capabilitiesHasPluginAuth = capabilities[Capabilities.CLIENT_PLUGIN_AUTH]
        val authPluginDataLength =
            if (capabilitiesHasPluginAuth) {
                buffer.readByteAsInt()
            } else {
                buffer.readByte()
                0
            }
        buffer.readBytes(6)

        if (capabilities[Capabilities.CLIENT_MYSQL]) {
            buffer.readBytes(4)
        } else {
            val capabilities3 = buffer.readIntLe().toLong()
            capabilities += Capabilities(capabilities3 shl 32)
        }

        val authPluginData2 =
            if (capabilities[Capabilities.CLIENT_SECURE_CONNECTION]) {
                val length = authPluginDataLength.coerceAtLeast(12)
                val value = buffer.readBytes(length)
                buffer.readByte()
                value
            } else {
                ByteArray(0)
            }

        val authPlugin =
            if (capabilitiesHasPluginAuth) {
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
