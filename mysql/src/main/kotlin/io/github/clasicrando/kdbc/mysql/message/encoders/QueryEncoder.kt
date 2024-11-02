package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

internal object QueryEncoder : MessageEncoder<MysqlMessage.Query, Unit> {
    override fun encode(
        value: MysqlMessage.Query,
        buffer: ByteWriteBuffer,
        context: Unit,
    ) {
        buffer.writeByte(0x03)
        buffer.writeText(value.sql)
    }
}
