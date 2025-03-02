package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.Sink
import kotlinx.io.writeString

/**
 * [MessageEncoder] for [MysqlMessage.Prepare]. Sends the supplied query for creating a prepared
 * statement.
 *
 * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html)
 */
internal object PrepareEncoder : MessageEncoder<MysqlMessage.Prepare, Unit> {
    override fun encode(value: MysqlMessage.Prepare, buffer: Sink, context: Unit) {
        buffer.writeByte(0x16)
        buffer.writeString(value.query)
    }
}
