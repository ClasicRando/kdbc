package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.message.PgMessage

internal object ParameterStatusDecoder : MessageDecoder<PgMessage.ParameterStatus> {
    override fun decode(buffer: ReadBuffer): PgMessage.ParameterStatus {
        return buffer.use {
            PgMessage.ParameterStatus(
                it.readCString(),
                it.readCString(),
            )
        }
    }
}
