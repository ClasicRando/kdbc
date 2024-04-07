package com.github.kdbc.postgresql.message.decoders

import com.github.kdbc.core.buffer.ByteReadBuffer
import com.github.kdbc.core.message.MessageDecoder
import com.github.kdbc.core.use
import com.github.kdbc.postgresql.message.PgMessage

/**
 * [MessageDecoder] for [PgMessage.ParameterDescription]. This message is sent as a response when
 * the frontend requests a description of a statement. The contents are:
 *
 * - the number of parameters as a [Short]
 * - for each parameter
 *     - the OID of the parameter data type
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-PARAMETERDESCRIPTION)
 */
internal object ParameterDescriptionDecoder : MessageDecoder<PgMessage.ParameterDescription> {
    override fun decode(buffer: ByteReadBuffer): PgMessage.ParameterDescription {
        return buffer.use { buf ->
            val parameterCount = buf.readShort()
            val parameterTypes = List(parameterCount.toInt()) { buf.readInt() }
            PgMessage.ParameterDescription(parameterTypes)
        }
    }
}
