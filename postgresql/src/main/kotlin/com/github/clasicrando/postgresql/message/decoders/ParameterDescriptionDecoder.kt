package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ReadBuffer
import com.github.clasicrando.common.buffer.readInt
import com.github.clasicrando.common.buffer.readShort
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.message.PgMessage

internal object ParameterDescriptionDecoder : MessageDecoder<PgMessage.ParameterDescription> {
    override fun decode(buffer: ReadBuffer): PgMessage.ParameterDescription {
        return buffer.use { buf ->
            val parameterCount = buf.readShort()
            val parameterTypes = List(parameterCount.toInt()) { buf.readInt() }
            PgMessage.ParameterDescription(parameterCount, parameterTypes)
        }
    }
}
