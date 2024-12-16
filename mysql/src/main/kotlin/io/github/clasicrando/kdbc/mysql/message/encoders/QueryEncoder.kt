package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeString
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

/**
 * [MessageEncoder] for [MysqlMessage.Query]. Sends the supplied query for execution without
 * parameters.
 *
 * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html)
 */
internal object QueryEncoder : MessageEncoder<MysqlMessage.Query, Unit> {
    override fun encode(value: MysqlMessage.Query, buffer: ByteWriteBuffer, context: Unit) {
        buffer.writeByte(0x03)
        buffer.writeString(value.sql)
    }
}
