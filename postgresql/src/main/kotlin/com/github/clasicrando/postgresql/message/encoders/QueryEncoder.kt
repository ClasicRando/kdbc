package com.github.clasicrando.postgresql.message.encoders

import com.github.clasicrando.common.buffer.ByteWriteBuffer
import com.github.clasicrando.common.message.MessageEncoder
import com.github.clasicrando.postgresql.message.PgMessage

/**
 * [MessageEncoder] for [PgMessage.Query]. This message is sent to request a simple query execution
 * (i.e. no parameters provided). The contents are:
 * - a header [Byte] of 'C'
 * - the length of the following data (including the size of the [Int] length)
 * - the query as a CString
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-QUERY)
 */
internal object QueryEncoder : MessageEncoder<PgMessage.Query> {
    override fun encode(value: PgMessage.Query, buffer: ByteWriteBuffer) {
        buffer.writeCode(value)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeCString(value.query)
        }
    }
}
