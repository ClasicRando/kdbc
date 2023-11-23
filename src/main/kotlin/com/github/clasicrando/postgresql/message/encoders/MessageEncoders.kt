package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.column.TypeRegistry
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset

class MessageEncoders(
    charset: Charset,
    typeRegistry: TypeRegistry,
) {
    private val startupMessageEncoder = StartupMessageEncoder(charset)
    private val saslInitialResponseEncoder = SaslInitialResponseEncoder(charset)
    private val saslResponseEncoder = SaslResponseEncoder(charset)
    private val queryEncoder = QueryEncoder(charset)
    private val parseMessageEncoder = ParseMessageEncoder(charset, typeRegistry)
    private val bindMessageEncoder = BindMessageEncoder(charset, typeRegistry)
    private val describeMessageEncoder = DescribeMessageEncoder(charset)
    private val executeMessageEncoder = ExecuteMessageEncoder(charset)
    private val closeMessageEncoder = CloseMessageEncoder(charset)

    @Suppress("UNCHECKED_CAST")
    fun <T : PgMessage> encoderFor(message: T): MessageEncoder<T> {
        return when (message) {
            is PgMessage.StartupMessage -> startupMessageEncoder
            is PgMessage.PasswordMessage -> PasswordMessageEncoder
            is PgMessage.SaslInitialResponse -> saslInitialResponseEncoder
            is PgMessage.SaslResponse -> saslResponseEncoder
            is PgMessage.SslRequest -> SslMessageEncoder
            is PgMessage.Query -> queryEncoder
            is PgMessage.Terminate -> CodeOnlyMessageEncoder
            is PgMessage.Parse -> parseMessageEncoder
            is PgMessage.Bind -> bindMessageEncoder
            is PgMessage.Describe -> describeMessageEncoder
            is PgMessage.Execute -> executeMessageEncoder
            is PgMessage.Sync -> CodeOnlyMessageEncoder
            is PgMessage.Close -> closeMessageEncoder
            else -> error("Message $message cannot be encoded")
        } as MessageEncoder<T>
    }
}
