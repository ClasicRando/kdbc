package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.exceptions.checkOrKdbcException
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.authentication.AuthPlugin
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

internal object AuthSwitchRequestDecoder : MessageDecoder<MysqlMessage.AuthSwitchRequest, Boolean> {
    @OptIn(ExperimentalStdlibApi::class)
    override fun decode(
        buffer: ByteReadBuffer,
        context: Boolean,
    ): MysqlMessage.AuthSwitchRequest {
        val header = buffer.readByteAsInt()
        checkOrKdbcException(header == 0xFE) {
            "Expected auth switch header (0xFE) but found ${header.toHexString()}"
        }

        val plugin = AuthPlugin.fromName(buffer.readCString())
        if (plugin == AuthPlugin.MySqlClearPassword) {
            if (!context) {
                throw KdbcException(
                    "Server expects mysql_clear_text_plugin but client has it disabled",
                )
            }
            if (buffer.remaining() == 0) {
                return MysqlMessage.AuthSwitchRequest(plugin, ByteArray(0))
            }
        }

        if (buffer.remaining() != 21) {
            throw KdbcException("Expected 21 bytes but found ${buffer.remaining()} bytes")
        }
        val data = buffer.readBytes(20)
        buffer.readByte()

        return MysqlMessage.AuthSwitchRequest(plugin, data)
    }
}
