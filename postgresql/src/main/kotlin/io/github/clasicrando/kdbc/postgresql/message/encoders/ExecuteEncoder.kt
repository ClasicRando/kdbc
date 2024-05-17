package io.github.clasicrando.kdbc.postgresql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeLengthPrefixed
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage

/**
 * [MessageEncoder] for [PgMessage.Execute]. This message is sent to execute a previously created
 * portal. The contents are:
 * - a header [Byte] of 'E'
 * - the length of the following data (including the size of the [Int] length)
 * - CString as the name of the portal (can be empty to close the unnamed portal)
 * - [Int] as the maximum number of rows to return if the portal returns rows (ignored otherwise).
 * Zero signifies that all rows are returned
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-EXECUTE)
 */
internal object ExecuteEncoder : MessageEncoder<PgMessage.Execute> {
    override fun encode(value: PgMessage.Execute, buffer: ByteWriteBuffer) {
        buffer.writeByte(value.code)
        buffer.writeLengthPrefixed(includeLength = true) {
            writeCString(value.portalName ?: "")
            writeInt(value.maxRowCount)
        }
    }
}
