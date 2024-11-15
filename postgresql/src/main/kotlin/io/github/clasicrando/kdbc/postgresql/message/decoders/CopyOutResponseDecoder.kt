package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.postgresql.copy.CopyFormat
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import kotlinx.io.Source

/**
 * [MessageDecoder] for [PgMessage.CopyOutResponse]. This message is sent to signify the backend has
 * acknowledged an initialization of a `COPY TO` operation. The client will then receive zero or
 * more [PgMessage.CopyData] messages as part of the `COPY TO` operation. The contents are:
 * - a [Byte] corresponding to the overall [CopyFormat]
 * - the number of columns in the incoming data as a [Short]
 * - the format of each column as a [List] of [Short] values
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COPYOUTRESPONSE)
 */
internal object CopyOutResponseDecoder : PgMessageDecoder<PgMessage.CopyOutResponse>() {
    override fun decode(buffer: Source): PgMessage.CopyOutResponse {
        val copyFormat = CopyFormat.fromByte(buffer.readByte())
        val columnCount = buffer.readShort().toInt()
        val columnFormats = List(columnCount) { CopyFormat.fromByte(buffer.readByte()) }

        return PgMessage.CopyOutResponse(copyFormat, columnCount, columnFormats)
    }
}
