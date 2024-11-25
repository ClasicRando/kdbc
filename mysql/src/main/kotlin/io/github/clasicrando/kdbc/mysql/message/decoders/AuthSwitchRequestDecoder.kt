package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.readCString
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.exceptions.checkOrKdbcException
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.authentication.AuthPlugin
import io.github.clasicrando.kdbc.mysql.buffer.readByteAsInt
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.ktor.utils.io.core.remaining
import kotlinx.io.Source
import kotlinx.io.readByteArray

internal object AuthSwitchRequestDecoder : MessageDecoder<MysqlMessage.AuthSwitchRequest, Boolean> {
    override fun decode(buffer: Source, context: Boolean): MysqlMessage.AuthSwitchRequest {
        val header = buffer.readByteAsInt()
        checkOrKdbcException(header == 0xFE) {
            "Expected auth switch header (0xFE) but found 0x${header.toHexString()}"
        }

        val plugin = AuthPlugin.fromName(buffer.readCString())
        if (plugin == AuthPlugin.MySqlClearPassword) {
            if (!context) {
                throw KdbcException(
                    "Server expects mysql_clear_text_plugin but client has it disabled"
                )
            }
            if (buffer.exhausted()) {
                return MysqlMessage.AuthSwitchRequest(plugin, ByteArray(0))
            }
        }

        if (!buffer.request(21)) {
            throw KdbcException("Expected 21 bytes but found ${buffer.remaining} bytes")
        }
        val data = buffer.readByteArray(20)
        buffer.readByte()

        return MysqlMessage.AuthSwitchRequest(plugin, data)
    }
}
