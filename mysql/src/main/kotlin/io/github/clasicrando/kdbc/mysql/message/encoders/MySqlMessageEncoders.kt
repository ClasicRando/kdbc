package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.DelicateIoApi
import kotlinx.io.Sink
import kotlinx.io.writeToInternalBuffer

internal object MySqlMessageEncoders {
    @OptIn(DelicateIoApi::class)
    fun encode(
        message: MysqlMessage,
        buffer: Sink,
        capabilities: Capabilities,
    ) {
        when (message) {
            is MysqlMessage.AuthSwitchResponse ->
                AuthSwitchResponseEncoder.encode(
                    message,
                    buffer,
                    capabilities,
                )
            is MysqlMessage.Execute -> ExecuteEncoder.encode(message, buffer, capabilities)
            is MysqlMessage.HandshakeResponse ->
                HandshakeResponseEncoder.encode(
                    message,
                    buffer,
                    capabilities,
                )
            MysqlMessage.Ping -> PingEncoder.encode(MysqlMessage.Ping, buffer, Unit)
            is MysqlMessage.Prepare -> PrepareEncoder.encode(message, buffer, Unit)
            is MysqlMessage.Query -> QueryEncoder.encode(message, buffer, Unit)
            MysqlMessage.Quit -> QuitEncoder.encode(MysqlMessage.Quit, buffer, Unit)
            is MysqlMessage.SslRequest -> SslRequestEncoder.encode(message, buffer, capabilities)
            is MysqlMessage.StatementClose -> StatementCloseEncoder.encode(message, buffer, Unit)
            is MysqlMessage.SingleByte -> buffer.writeByte(message.byte)
            is MysqlMessage.MultiByte -> buffer.write(message.bytes)
            is MysqlMessage.ResetSession -> buffer.writeByte(0x1f)
            is MysqlMessage.LoadLocal -> buffer.writeToInternalBuffer {
                it.write(message.source, message.source.size)
            }
            is MysqlMessage.Empty -> {} // write nothing
            else -> throw KdbcException("Could not match encoder to message: $message")
        }
    }
}
