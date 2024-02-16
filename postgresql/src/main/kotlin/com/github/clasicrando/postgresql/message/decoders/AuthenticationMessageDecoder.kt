package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.common.buffer.readBytes
import com.github.clasicrando.common.buffer.readFully
import com.github.clasicrando.common.buffer.readInt
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.splitAsCString
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.authentication.Authentication
import com.github.clasicrando.postgresql.message.PgMessage

internal object AuthenticationMessageDecoder : MessageDecoder<PgMessage.Authentication> {
    private fun decodeAuth(buffer: ReadBuffer): Authentication {
        return when (val method = buffer.readInt()) {
            0 -> Authentication.Ok
            // Kerberos Auth does not appear to still be supported or the docs cannot be found
//            2 ->
            3 -> Authentication.CleartextPassword
            5 -> Authentication.Md5Password(salt = buffer.readBytes(4))
//            7 -> Authentication.Gss
//            8 -> Authentication.KerberosV5
            10 -> {
                val bytes = buffer.readFully()
                Authentication.Sasl(bytes.splitAsCString())
            }
            11 -> Authentication.SaslContinue(
                String(bytes = buffer.readFully())
            )
            12 -> Authentication.SaslFinal(
                saslData = String(bytes = buffer.readFully())
            )
            else -> {
                error("Unknown authentication method: $method")
            }
        }
    }

    override fun decode(buffer: ReadBuffer): PgMessage.Authentication {
        val auth: Authentication = buffer.use { decodeAuth(it) }
        return PgMessage.Authentication(auth)
    }
}
