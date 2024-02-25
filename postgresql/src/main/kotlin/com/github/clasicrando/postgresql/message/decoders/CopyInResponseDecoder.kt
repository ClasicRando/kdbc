package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.copy.CopyFormat
import com.github.clasicrando.postgresql.message.PgMessage

internal object CopyInResponseDecoder : MessageDecoder<PgMessage.CopyInResponse> {
    override fun decode(buffer: ByteReadBuffer): PgMessage.CopyInResponse {
        return buffer.use { buf ->
            val copyFormat = CopyFormat.fromByte(buf.readByte())
            val columnCount = buf.readShort().toInt()
            val columnFormats = Array(columnCount) {
                CopyFormat.fromByte(buf.readByte())
            }

            PgMessage.CopyInResponse(copyFormat, columnCount, columnFormats)
        }
    }
}
