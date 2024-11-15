package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.ZERO_BYTE
import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.message.information.InformationResponse

/**
 * Generic decoder for messages that contains similarly structured messages. The contents 1 or more
 * error code and [String] value pairs where the key is a [Byte] (converted to a [Char] for our
 * purposes) and the value is a CString.
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-error-fields.html)
 */
internal abstract class InformationResponseDecoder<T : PgMessage> : PgMessageDecoder<T>() {
    fun decodeToInformationResponse(buffer: ByteReadBuffer): InformationResponse =
        buffer.use { buf ->
            val map = buildMap {
                while (buf.remaining() > 0) {
                    val kind = buf.readByte()
                    if (kind != ZERO_BYTE) {
                        put(kind, buf.readCString())
                    }
                }
            }
            InformationResponse(map)
        }
}
