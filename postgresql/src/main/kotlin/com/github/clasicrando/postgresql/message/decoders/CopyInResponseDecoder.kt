package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.copy.CopyFormat
import com.github.clasicrando.postgresql.message.PgMessage

/**
 * [MessageDecoder] for [PgMessage.CopyInResponse]. This message is sent to signify the backend has
 * acknowledged an initialization of a `COPY FROM` operation. The client can therefore send
 * [PgMessage.CopyData] messages as part of the `COPY FROM` operation. The contents are:
 *
 * - a [Byte] corresponding to the overall [CopyFormat]
 * - the number of columns in the incoming data as a [Short]
 * - the format of each column as a [List] of [Short] values
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COPYINRESPONSE)
 */
internal object CopyInResponseDecoder : MessageDecoder<PgMessage.CopyInResponse> {
    override fun decode(buffer: ByteReadBuffer): PgMessage.CopyInResponse {
        return buffer.use { buf ->
            val copyFormat = CopyFormat.fromByte(buf.readByte())
            val columnCount = buf.readShort().toInt()
            val columnFormats = List(columnCount) {
                CopyFormat.fromByte(buf.readByte())
            }

            PgMessage.CopyInResponse(copyFormat, columnCount, columnFormats)
        }
    }
}
