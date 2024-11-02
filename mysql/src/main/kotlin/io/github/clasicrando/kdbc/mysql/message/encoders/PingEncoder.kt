package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

internal object PingEncoder : MessageEncoder<MysqlMessage.Ping, Unit> {
    override fun encode(
        value: MysqlMessage.Ping,
        buffer: ByteWriteBuffer,
        context: Unit,
    ) {
        buffer.writeByte(0x0e)
    }
}
