package io.github.clasicrando.kdbc.mysql.message.decoders

import io.github.clasicrando.kdbc.core.message.MessageDecoder
import io.github.clasicrando.kdbc.mysql.buffer.readByteAsInt
import io.github.clasicrando.kdbc.mysql.buffer.readLongLengthEncoded
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import io.github.clasicrando.kdbc.mysql.result.MySqlColumn
import io.github.clasicrando.kdbc.mysql.result.MySqlValue
import kotlinx.io.Source
import kotlinx.io.readByteArray

internal object TextRowDecoder : MessageDecoder<MysqlMessage.TextRow, List<MySqlColumn>> {
    override fun decode(buffer: Source, context: List<MySqlColumn> ): MysqlMessage.TextRow {
        val values: Array<MySqlValue?> =
            Array(context.size) { i ->
                if (buffer.peek().readByteAsInt() == 0xfb) {
                    buffer.readByte()
                    return@Array null
                }

                val size = buffer.readLongLengthEncoded().toInt()
                MySqlValue.Text(buffer.readByteArray(size).toString(Charsets.UTF_8), context[i])
            }
        return MysqlMessage.TextRow(values)
    }
}
