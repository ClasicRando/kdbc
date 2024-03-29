package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage

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
