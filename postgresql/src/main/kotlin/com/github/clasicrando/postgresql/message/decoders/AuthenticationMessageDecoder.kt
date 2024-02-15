package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.splitAsCString
import com.github.clasicrando.postgresql.authentication.Authentication
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.core.ByteReadPacket
import io.ktor.utils.io.core.readBytes
import io.ktor.utils.io.core.readInt

internal object AuthenticationMessageDecoder : MessageDecoder<PgMessage.Authentication> {
    override fun decode(packet: ByteReadPacket): PgMessage.Authentication {
        val auth: Authentication = when (val method = packet.readInt()) {
            0 -> Authentication.Ok
            // Kerberos Auth does not appear to still be supported or the docs cannot be found
//            2 ->
            3 -> Authentication.CleartextPassword
            5 -> Authentication.Md5Password(salt = packet.readBytes(4))
//            7 -> Authentication.Gss
//            8 -> Authentication.KerberosV5
            10 -> {
                val bytes = packet.readBytes()
                Authentication.Sasl(bytes.splitAsCString())
            }
            11 -> Authentication.SaslContinue(
                String(bytes = packet.readBytes())
            )
            12 -> Authentication.SaslFinal(
                saslData = String(bytes = packet.readBytes())
            )
            else -> {
                error("Unknown authentication method: $method")
            }
        }
        return PgMessage.Authentication(auth)
    }
}
