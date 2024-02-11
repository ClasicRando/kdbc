package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage

internal class PgMessageEncoders {
    @Suppress("UNCHECKED_CAST")
    fun <T : PgMessage> encoderFor(message: T): MessageEncoder<T> {
        return when (message) {
            is PgMessage.StartupMessage -> StartupEncoder
            is PgMessage.PasswordMessage -> PasswordEncoder
            is PgMessage.SaslInitialResponse -> SaslInitialResponseEncoder
            is PgMessage.SaslResponse -> SaslResponseEncoder
            is PgMessage.SslRequest -> SslMessageEncoder
            is PgMessage.Query -> QueryEncoder
            is PgMessage.Terminate -> CodeOnlyMessageEncoder
            is PgMessage.Parse -> ParseEncoder
            is PgMessage.Bind -> BindEncoder
            is PgMessage.Describe -> DescribeEncoder
            is PgMessage.Execute -> ExecuteEncoder
            is PgMessage.Sync -> CodeOnlyMessageEncoder
            is PgMessage.Close -> CloseEncoder
            is PgMessage.CopyData -> CopyDataEncoder
            is PgMessage.CopyDone -> CodeOnlyMessageEncoder
            is PgMessage.CopyFail -> CopyFailEncoder
            else -> error("Message $message cannot be encoded")
        } as MessageEncoder<T>
    }
}
