package com.github.clasicrando.postgresql.message.decoders

import com.github.clasicrando.common.buffer.ByteReadBuffer
import com.github.clasicrando.common.message.MessageDecoder
import com.github.clasicrando.common.use
import com.github.clasicrando.postgresql.message.PgMessage

/**
 * [MessageDecoder] for [PgMessage.BackendKeyData]. This message is sent as a response when the
 * frontend requests a description of a statement. The contents are:
 *
 * - the number of parameters as a [Short]
 * - for each parameter
 *     - the Oid of the parameter data type
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-PARAMETERDESCRIPTION)
 */
internal object ParameterDescriptionDecoder : MessageDecoder<PgMessage.ParameterDescription> {
    override fun decode(buffer: ByteReadBuffer): PgMessage.ParameterDescription {
        return buffer.use { buf ->
            val parameterCount = buf.readShort()
            val parameterTypes = List(parameterCount.toInt()) { buf.readInt() }
            PgMessage.ParameterDescription(parameterCount, parameterTypes)
        }
    }
}
