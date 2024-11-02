package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

internal object MySqlMessageEncoders {
    fun encode(
        message: MysqlMessage,
        buffer: ByteWriteBuffer,
        capabilities: Capabilities,
        sequenceId: Byte,
    ): Byte {
        when (message) {
            is MysqlMessage.AuthSwitchResponse ->
                AuthSwitchResponseEncoder.encode(
                    message,
                    buffer,
                    capabilities,
                )
            is MysqlMessage.BinaryRow -> TODO()
            is MysqlMessage.Eof -> TODO()
            is MysqlMessage.Err -> TODO()
            is MysqlMessage.Execute -> TODO()
            is MysqlMessage.HandshakeResponse ->
                HandshakeResponseEncoder.encode(
                    message,
                    buffer,
                    capabilities,
                )
            is MysqlMessage.ColumnDefinition -> TODO()
            is MysqlMessage.Ok -> TODO()
            MysqlMessage.Ping -> TODO()
            is MysqlMessage.Prepare -> TODO()
            is MysqlMessage.PrepareOk -> TODO()
            is MysqlMessage.Query -> TODO()
            MysqlMessage.Quit -> TODO()
            is MysqlMessage.SslRequest -> SslRequestEncoder.encode(message, buffer, capabilities)
            is MysqlMessage.StatementClose -> TODO()
            is MysqlMessage.TextRow -> TODO()
            else -> throw KdbcException("Could not match encoder to message: $message")
        }
    }
}
