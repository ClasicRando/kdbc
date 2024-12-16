package io.github.clasicrando.kdbc.postgresql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.postgresql.exceptions.PgException
import io.github.clasicrando.kdbc.postgresql.message.PgMessage

/** Common entry point for encoding frontend [PgMessage]s. */
internal object PgMessageEncoders {
    /**
     * Encode the [PgMessage] into the supplied [buffer] by looking up the appropriate
     * [MessageEncoder] and calling [MessageEncoder.encode].
     *
     * @throws KdbcException if the [message] provided does not have a corresponding
     *   [MessageEncoder]
     */
    fun encode(message: PgMessage, buffer: ByteWriteBuffer) {
        when (message) {
            is PgMessage.StartupMessage -> StartupEncoder.encode(message, buffer)
            is PgMessage.PasswordMessage -> PasswordEncoder.encode(message, buffer)
            is PgMessage.SaslInitialResponse -> SaslInitialResponseEncoder.encode(message, buffer)
            is PgMessage.SaslResponse -> SaslResponseEncoder.encode(message, buffer)
            is PgMessage.SslRequest -> SslMessageEncoder.encode(message, buffer)
            is PgMessage.Query -> QueryEncoder.encode(message, buffer)
            is PgMessage.Terminate -> CodeOnlyMessageEncoder.encode(message, buffer)
            is PgMessage.Parse -> ParseEncoder.encode(message, buffer)
            is PgMessage.Bind -> BindEncoder.encode(message, buffer)
            is PgMessage.Describe -> DescribeEncoder.encode(message, buffer)
            is PgMessage.Execute -> ExecuteEncoder.encode(message, buffer)
            is PgMessage.Sync -> CodeOnlyMessageEncoder.encode(message, buffer)
            is PgMessage.Close -> CloseEncoder.encode(message, buffer)
            is PgMessage.CopyData -> CopyDataEncoder.encode(message, buffer)
            is PgMessage.CopyDone -> CodeOnlyMessageEncoder.encode(message, buffer)
            is PgMessage.CopyFail -> CopyFailEncoder.encode(message, buffer)
            is PgMessage.CancelRequest -> CancelRequestEncoder.encode(message, buffer)
            else -> throw PgException("Message $message cannot be encoded")
        }
    }
}
