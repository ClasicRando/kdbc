package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.buffer.ByteReadBuffer
import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.readLongLengthEncoded
import io.github.clasicrando.kdbc.mysql.result.MySqlColumn
import io.github.clasicrando.kdbc.mysql.result.MySqlDataRow
import io.github.clasicrando.kdbc.mysql.result.MySqlValue

internal object TextRowDecoder : MessageDecoder<MysqlMessage.TextRow, List<MySqlColumn>> {
    override fun decode(
        buffer: ByteReadBuffer,
        context: List<MySqlColumn>,
    ): MysqlMessage.TextRow {
        val values: Array<MySqlValue?> =
            Array(context.size) {
                if ((buffer.peekNext().toInt() and 0xff) == 0xfb) {
                    buffer.readByte()
                    return@Array null
                }

                val size = buffer.readLongLengthEncoded().toInt()
                MySqlValue.Text(buffer.readBytes(size).toString(Charsets.UTF_8))
            }
        return MysqlMessage.TextRow(MySqlDataRow(values))
    }
}
