package io.github.clasicrando.kdbc.postgresql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.postgresql.message.PgMessage

/**
 * [MessageDecoder] for [PgMessage.DataRow]. This message is sent as part of a query result and
 * represents a single row of the result. Although the buffer contents are not parsed right away,
 * the structure is:
 *
 * - the number of column values as a [Short] (possible zero)
 * - for each column
 *     - the length of the column value in bytes as a [Short] (-1 is a special value for null)
 *     - the value of column as a [ByteArray] with a size that equals the previous value
 *
 * [docs](https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-DATAROW)
 */
internal object DataRowDecoder : MessageDecoder<PgMessage.DataRow> {
    override fun decode(buffer: ByteReadBuffer): PgMessage.DataRow {
        return PgMessage.DataRow(buffer)
    }
}
