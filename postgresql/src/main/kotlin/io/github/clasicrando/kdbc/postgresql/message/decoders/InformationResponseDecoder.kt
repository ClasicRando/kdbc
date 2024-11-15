package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.ZERO_BYTE
import io.github.clasicrando.kdbc.core.buffer.readCString
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import io.github.clasicrando.kdbc.postgresql.message.information.InformationResponse
import kotlinx.io.Source

/**
 * Generic decoder for messages that contains similarly structured messages. The contents 1 or more
 * error code and [String] value pairs where the key is a [Byte] (converted to a [Char] for our
 * purposes) and the value is a CString.
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-error-fields.html)
 */
internal abstract class InformationResponseDecoder<T : PgMessage> : PgMessageDecoder<T>() {
    fun decodeToInformationResponse(buffer: Source): InformationResponse {
        val map = buildMap {
            while (!buffer.exhausted()) {
                val kind = buffer.readByte()
                if (kind != ZERO_BYTE) {
                    put(kind, buffer.readCString())
                }
            }
        }
        return InformationResponse(map)
    }
}
