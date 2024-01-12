package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.column.PgTypeRegistry
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset

internal class PgMessageEncoders(
    charset: Charset,
    typeRegistry: PgTypeRegistry,
) {
    private val startupEncoder = StartupEncoder(charset)
    private val saslInitialResponseEncoder = SaslInitialResponseEncoder(charset)
    private val saslResponseEncoder = SaslResponseEncoder(charset)
    private val queryEncoder = QueryEncoder(charset)
    private val parseEncoder = ParseEncoder(charset, typeRegistry)
    private val bindEncoder = BindEncoder(charset, typeRegistry)
    private val describeEncoder = DescribeEncoder(charset)
    private val executeEncoder = ExecuteEncoder(charset)
    private val closeEncoder = CloseEncoder(charset)
    private val copyFailMessageEncoder = CopyFailEncoder(charset)

    @Suppress("UNCHECKED_CAST")
    fun <T : PgMessage> encoderFor(message: T): MessageEncoder<T> {
        return when (message) {
            is PgMessage.StartupMessage -> startupEncoder
            is PgMessage.PasswordMessage -> PasswordEncoder
            is PgMessage.SaslInitialResponse -> saslInitialResponseEncoder
            is PgMessage.SaslResponse -> saslResponseEncoder
            is PgMessage.SslRequest -> SslMessageEncoder
            is PgMessage.Query -> queryEncoder
            is PgMessage.Terminate -> CodeOnlyMessageEncoder
            is PgMessage.Parse -> parseEncoder
            is PgMessage.Bind -> bindEncoder
            is PgMessage.Describe -> describeEncoder
            is PgMessage.Execute -> executeEncoder
            is PgMessage.Sync -> CodeOnlyMessageEncoder
            is PgMessage.Close -> closeEncoder
            is PgMessage.CopyData -> CopyDataEncoder
            is PgMessage.CopyDone -> CodeOnlyMessageEncoder
            is PgMessage.CopyFail -> copyFailMessageEncoder
            else -> error("Message $message cannot be encoded")
        } as MessageEncoder<T>
    }
}
