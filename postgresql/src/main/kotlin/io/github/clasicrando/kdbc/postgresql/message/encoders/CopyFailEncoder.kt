package io.github.clasicrando.kdbc.postgresql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage

/**
 * [MessageEncoder] for [PgMessage.CopyFail]. This message is sent to notify the backend that the
 * `COPY FROM` operation should be aborted. The contents are:
 * - a header [Byte] of 'f'
 * - the length of the following data (including the size of the [Int] length)
 * - CString as an error message to report the failure cause
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COPYFAIL)
 */
internal object CopyFailEncoder : MessageEncoder<PgMessage.CopyFail> {
    override fun encode(value: PgMessage.CopyFail, buffer: ByteWriteBuffer) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeCString(value.message)
        }
    }
}
