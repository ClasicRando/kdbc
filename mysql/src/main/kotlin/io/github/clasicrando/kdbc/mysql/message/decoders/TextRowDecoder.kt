package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.buffer.readByteAsInt
import io.github.clasicrando.kdbc.mysql.buffer.readBytesLengthEncoded
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.result.MySqlColumn
import io.github.clasicrando.kdbc.mysql.result.MySqlValue
import kotlinx.io.Source

/**
 * [MessageDecoder] for [MysqlMessage.TextRow] packets. This packet has no header byte and it simply
 * a collection of bytes that can be sliced by reading length encoded values. When starting on a new
 * value, if the first byte is 0xfb that indicates a null value so that byte should be discarded and
 * a null value is packed into the row.
 *
 * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_row.html)
 */
internal object TextRowDecoder : MessageDecoder<MysqlMessage.TextRow, List<MySqlColumn>> {
    override fun decode(buffer: Source, context: List<MySqlColumn>): MysqlMessage.TextRow {
        val values: Array<MySqlValue?> =
            Array(context.size) { i ->
                if (buffer.peek().readByteAsInt() == 0xfb) {
                    buffer.readByte()
                    return@Array null
                }

                val bytes = buffer.readBytesLengthEncoded()
                MySqlValue.Text(ByteReadBuffer(bytes), context[i])
            }
        return MysqlMessage.TextRow(values)
    }
}
