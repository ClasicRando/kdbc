package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.connection.LongBitFlags
import io.github.clasicrando.kdbc.mysql.exceptions.MySqlException
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.DelicateIoApi
import kotlinx.io.Sink
import kotlinx.io.writeToInternalBuffer

internal object MySqlMessageEncoders {
    @OptIn(DelicateIoApi::class)
    fun encode(message: MysqlMessage, buffer: Sink, capabilities: LongBitFlags) {
        when (message) {
            // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_auth_switch_response.html
            is MysqlMessage.AuthSwitchResponse -> buffer.write(message.bytes)
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
            is MysqlMessage.MultiByte -> buffer.write(message.bytes)
            // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_reset_connection.html
            is MysqlMessage.ResetSession -> buffer.writeByte(0x1f)
            // https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_local_infile_data.html
            is MysqlMessage.LoadLocal ->
                buffer.writeToInternalBuffer { it.write(message.source, message.source.size) }
            is MysqlMessage.Empty -> {} // write nothing
            else -> throw MySqlException("Could not match encoder to message: $message")
        }
    }
}
