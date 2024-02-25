package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.splitAsCString
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.authentication.Authentication
import com.github.clasicrando.postgresql.message.PgMessage

internal object AuthenticationMessageDecoder : MessageDecoder<PgMessage.Authentication> {
    private fun decodeAuth(buffer: ByteReadBuffer): Authentication {
        return when (val method = buffer.readInt()) {
            0 -> Authentication.Ok
            // Kerberos Auth does not appear to still be supported or the docs cannot be found
//            2 ->
            3 -> Authentication.CleartextPassword
            5 -> Authentication.Md5Password(salt = buffer.readBytes(4))
//            7 -> Authentication.Gss
//            8 -> Authentication.KerberosV5
            10 -> {
                val bytes = buffer.readBytes()
                Authentication.Sasl(bytes.splitAsCString())
            }
            11 -> Authentication.SaslContinue(
                String(bytes = buffer.readBytes())
            )
            12 -> Authentication.SaslFinal(
                saslData = String(bytes = buffer.readBytes())
            )
            else -> {
                error("Unknown authentication method: $method")
            }
        }
    }

    override fun decode(buffer: ByteReadBuffer): PgMessage.Authentication {
        val auth: Authentication = buffer.use { decodeAuth(it) }
        return PgMessage.Authentication(auth)
    }
}
