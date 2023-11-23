package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.ZERO
import com.github.clasicrando.postgresql.authentication.Authentication
import com.github.clasicrando.postgresql.message.PgMessage
import io.ktor.utils.io.charsets.Charset
import io.ktor.utils.io.core.ByteReadPacket
import io.ktor.utils.io.core.readBytes
import io.ktor.utils.io.core.readInt

class AuthenticationMessageDecoder(
    private val charset: Charset,
) : MessageDecoder<PgMessage.Authentication> {
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
                val mechanisms = buildList {
                    val builder = StringBuilder()
                    for (byte in bytes) {
                        if (byte != Byte.ZERO) {
                            builder.append(byte.toInt().toChar())
                        } else {
                            builder.takeIf { it.isNotEmpty() }?.let {
                                this.add(it.toString())
                            }
                            builder.clear()
                        }
                    }
                }
                Authentication.Sasl(mechanisms)
            }
            11 -> Authentication.SaslContinue(
                String(bytes = packet.readBytes(), charset = charset)
            )
            12 -> Authentication.SaslFinal(
                saslData = String(bytes = packet.readBytes(), charset = charset)
            )
            else -> {
                error("Unknown authentication method: $method")
            }
        }
        return PgMessage.Authentication(auth)
    }
}
