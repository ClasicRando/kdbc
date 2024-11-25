package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.Sink

internal object MySqlMessageEncoders {
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
            is MysqlMessage.ColumnDefinition -> TODO()
            MysqlMessage.Ping -> PingEncoder.encode(MysqlMessage.Ping, buffer, Unit)
            is MysqlMessage.Prepare -> PrepareEncoder.encode(message, buffer, Unit)
            is MysqlMessage.Query -> QueryEncoder.encode(message, buffer, Unit)
            MysqlMessage.Quit -> QuitEncoder.encode(MysqlMessage.Quit, buffer, Unit)
            is MysqlMessage.SslRequest -> SslRequestEncoder.encode(message, buffer, capabilities)
            is MysqlMessage.StatementClose -> StatementCloseEncoder.encode(message, buffer, Unit)
            is MysqlMessage.SingleByte -> SingleByteEncoder.encode(message, buffer, Unit)
            is MysqlMessage.MultiByte -> MultiByteEncoder.encode(message, buffer, Unit)
            else -> throw KdbcException("Could not match encoder to message: $message")
        }
    }
}
