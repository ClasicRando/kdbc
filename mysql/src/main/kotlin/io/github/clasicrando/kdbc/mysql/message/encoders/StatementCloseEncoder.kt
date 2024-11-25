package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.Sink
import kotlinx.io.writeIntLe

internal object StatementCloseEncoder : MessageEncoder<MysqlMessage.StatementClose, Unit> {
    override fun encode(value: MysqlMessage.StatementClose, buffer: Sink, context: Unit) {
        buffer.writeByte(0x19)
        buffer.writeIntLe(value.statementId)
    }
}
