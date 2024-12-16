package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

/**
 * [MessageEncoder] for [MysqlMessage.StatementClose]. Instructs the server to close a statement
 * with the specified ID.
 *
 * [docs](https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute.html)
 */
internal object StatementCloseEncoder : MessageEncoder<MysqlMessage.StatementClose, Unit> {
    override fun encode(
        value: MysqlMessage.StatementClose,
        buffer: ByteWriteBuffer,
        context: Unit,
    ) {
        buffer.writeByte(0x19)
        buffer.writeIntLe(value.statementId)
    }
}
