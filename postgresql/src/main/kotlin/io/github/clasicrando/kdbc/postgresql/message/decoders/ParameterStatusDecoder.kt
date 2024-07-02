package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage

/**
 * [MessageDecoder] for [PgMessage.ParameterStatus]. This message is sent when the session has been
 * authenticated but before the [PgMessage.BackendKeyData] and [PgMessage.ReadyForQuery] messages.
 * Each message contains a single run-time parameter name, and its current value. The contents are:
 *
 * - the name of the parameter as a CString
 * - the current value of the parameter as a CString
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-PARAMETERSTATUS)
 */
internal object ParameterStatusDecoder : MessageDecoder<PgMessage.ParameterStatus> {
    override fun decode(buffer: ByteReadBuffer): PgMessage.ParameterStatus {
        return buffer.use {
            PgMessage.ParameterStatus(
                it.readCString(),
                it.readCString(),
            )
        }
    }
}
