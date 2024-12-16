package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.connection.LongBitFlags
import io.github.clasicrando.kdbc.mysql.exceptions.MySqlException
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

internal object MySqlMessageEncoders {
    fun encode(message: MysqlMessage, buffer: ByteWriteBuffer, capabilities: LongBitFlags) {
        when (message) {
            // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_response.html
            is MysqlMessage.AuthSwitchResponse -> buffer.writeBytes(message.bytes)
            is MysqlMessage.Execute -> ExecuteEncoder.encode(message, buffer, capabilities)
            is MysqlMessage.HandshakeResponse ->
                HandshakeResponseEncoder.encode(message, buffer, capabilities)
            MysqlMessage.Ping -> buffer.writeByte(0x0e)
            is MysqlMessage.Prepare -> PrepareEncoder.encode(message, buffer, Unit)
            is MysqlMessage.Query -> QueryEncoder.encode(message, buffer, Unit)
            MysqlMessage.Quit -> buffer.writeByte(0x01)
            is MysqlMessage.SslRequest -> SslRequestEncoder.encode(message, buffer, capabilities)
            is MysqlMessage.StatementClose -> StatementCloseEncoder.encode(message, buffer, Unit)
            is MysqlMessage.SingleByte -> buffer.writeByte(message.byte)
            is MysqlMessage.MultiByte -> buffer.writeBytes(message.bytes)
            // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_reset_connection.html
            is MysqlMessage.ResetSession -> buffer.writeByte(0x1f)
            // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_local_infile_data.html
            is MysqlMessage.LoadLocal -> buffer.writeBuffer(message.data)
            is MysqlMessage.Empty -> {} // write nothing
            else -> throw MySqlException("Could not match encoder to message: $message")
        }
    }
}
