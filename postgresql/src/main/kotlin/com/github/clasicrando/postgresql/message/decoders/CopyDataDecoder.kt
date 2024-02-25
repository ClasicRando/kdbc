package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.message.PgMessage

internal object CopyDataDecoder : MessageDecoder<PgMessage.CopyData> {
    override fun decode(buffer: ByteReadBuffer): PgMessage.CopyData {
        return PgMessage.CopyData(buffer.use { it.readBytes() })
    }
}
