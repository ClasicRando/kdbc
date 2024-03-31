package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.message.PgMessage
import com.github.clasicrando.postgresql.message.information.InformationResponse

/**
 * Generic decoder for messages that contains similarly structured messages. The contents 1 or more
 * error code and [String] value pairs where the key is a [Byte] (converted to a [Char] for our
 * purposes) and the value is a CString.
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-error-fields.html)
 */
internal abstract class InformationResponseDecoder<T : PgMessage> : MessageDecoder<T> {
    fun decodeToInformationResponse(buffer: ByteReadBuffer): InformationResponse {
        return buffer.use { buf ->
            val map = buildMap {
                while (buf.remaining > 0) {
                    val kind = buf.readByte()
                    if (kind != ByteReadBuffer.ZERO_BYTE) {
                        put(kind, buf.readCString())
                    }
                }
            }
            InformationResponse(map)
        }
    }
}
