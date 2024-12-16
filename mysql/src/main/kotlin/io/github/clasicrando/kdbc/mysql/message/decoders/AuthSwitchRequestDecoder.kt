package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.authentication.AuthPlugin
import io.github.clasicrando.kdbc.mysql.exceptions.checkOrMySqlException
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

/**
 * [MessageDecoder] for [MysqlMessage.AuthSwitchRequest] packets. Starts with 0xfe followed by a
 * null terminated plugin name and an end of packet length byte data as initial authentication data
 * for the client plugin.
 *
 * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_request.html)
 */
internal object AuthSwitchRequestDecoder : MessageDecoder<MysqlMessage.AuthSwitchRequest, Boolean> {
    override fun decode(buffer: ByteReadBuffer, context: Boolean): MysqlMessage.AuthSwitchRequest {
        val header = buffer.readByteAsInt()
        checkOrMySqlException(header == 0xfe) {
            "Expected auth switch header (0xfe) but found 0x${header.toHexString()}"
        }

        val plugin = AuthPlugin.fromName(buffer.readCString())
        if (plugin == AuthPlugin.MySqlClearPassword) {
            checkOrMySqlException(context) {
                "Server expects mysql_clear_text_plugin but client has it disabled"
            }
            if (buffer.isExhausted) {
                return MysqlMessage.AuthSwitchRequest(plugin, ByteArray(0))
            }
        }

        checkOrMySqlException(buffer.request(21)) {
            "Expected 21 bytes but found ${buffer.remaining} bytes"
        }
        val data = buffer.readBytes(20)
        buffer.readByte()

        return MysqlMessage.AuthSwitchRequest(plugin, data)
    }
}
