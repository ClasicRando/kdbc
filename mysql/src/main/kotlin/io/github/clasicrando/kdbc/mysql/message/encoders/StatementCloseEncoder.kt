package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeIntLe
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

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
