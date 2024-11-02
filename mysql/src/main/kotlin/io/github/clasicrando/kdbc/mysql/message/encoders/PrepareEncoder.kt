package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

internal object PrepareEncoder : MessageEncoder<MysqlMessage.Prepare, Unit> {
    override fun encode(
        value: MysqlMessage.Prepare,
        buffer: ByteWriteBuffer,
        context: Unit,
    ) {
        buffer.writeByte(0x16)
        buffer.writeText(value.query)
    }
}
