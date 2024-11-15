package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage
import kotlinx.io.Source

/**
 * [MessageDecoder] for [PgMessage.ParameterDescription]. This message is sent as a response when
 * the frontend requests a description of a statement. The contents are:
 * - the number of parameters as a [Short]
 * - for each parameter
 *     - the OID of the parameter data type
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-PARAMETERDESCRIPTION)
 */
internal object ParameterDescriptionDecoder : PgMessageDecoder<PgMessage.ParameterDescription>() {
    override fun decode(buffer: Source): PgMessage.ParameterDescription {
        val parameterCount = buffer.readShort()
        val parameterTypes = List(parameterCount.toInt()) { buffer.readInt() }
        return PgMessage.ParameterDescription(parameterTypes)
    }
}
