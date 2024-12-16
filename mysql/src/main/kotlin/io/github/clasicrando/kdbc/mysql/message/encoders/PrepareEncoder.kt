package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeString
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

/**
 * [MessageEncoder] for [MysqlMessage.Prepare]. Sends the supplied query for creating a prepared
 * statement.
 *
 * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html)
 */
internal object PrepareEncoder : MessageEncoder<MysqlMessage.Prepare, Unit> {
    override fun encode(value: MysqlMessage.Prepare, buffer: ByteWriteBuffer, context: Unit) {
        buffer.writeByte(0x16)
        buffer.writeString(value.query)
    }
}
