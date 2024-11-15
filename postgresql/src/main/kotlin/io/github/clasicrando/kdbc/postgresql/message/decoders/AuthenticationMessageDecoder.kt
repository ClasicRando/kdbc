package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.exceptions.KdbcException
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.core.splitAsCString
import io.github.clasicrando.kdbc.postgresql.authentication.Authentication
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import kotlinx.io.Source
import kotlinx.io.readByteArray
import kotlinx.io.readString

/**
 * [MessageDecoder] for [PgMessage.Authentication] messages. All authentication message contents
 * start with an [Int] which designates which authentication message type is specified. Depending on
 * the message type, more data might follow.
 *
 * For all authentication messages, search for "Byte1('R')"
 * [here](https://www.postgresql.org/docs/current/protocol-message-formats.html)
 */
internal object AuthenticationMessageDecoder : PgMessageDecoder<PgMessage.Authentication>() {
    override fun decode(buffer: Source): PgMessage.Authentication {
        val auth: Authentication =
            buffer.use {
                when (val method = it.readInt()) {
                    0 -> Authentication.Ok
                    // Kerberos Auth does not appear to still be supported or the docs cannot be
                    // found
                    //                2 ->
                    3 -> Authentication.CleartextPassword
                    5 -> Authentication.Md5Password(salt = buffer.readByteArray(4))
                    //                7 -> Authentication.Gss
                    //                8 -> Authentication.KerberosV5
                    10 -> {
                        val bytes = buffer.readByteArray()
                        Authentication.Sasl(bytes.splitAsCString())
                    }
                    11 -> Authentication.SaslContinue(buffer.readString())
                    12 -> Authentication.SaslFinal(saslData = buffer.readString())
                    else -> throw KdbcException("Unknown authentication method: $method")
                }
            }
        return PgMessage.Authentication(auth)
    }
}
