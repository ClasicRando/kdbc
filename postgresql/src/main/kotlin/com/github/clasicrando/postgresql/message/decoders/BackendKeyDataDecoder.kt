package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.common.buffer.readInt
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.message.PgMessage

internal object BackendKeyDataDecoder : MessageDecoder<PgMessage.BackendKeyData> {
    override fun decode(buffer: ReadBuffer): PgMessage.BackendKeyData {
        return buffer.use {
            PgMessage.BackendKeyData(it.readInt(), it.readInt())
        }
    }
}
