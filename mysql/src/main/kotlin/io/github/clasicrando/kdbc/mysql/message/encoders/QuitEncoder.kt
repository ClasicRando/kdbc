package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

internal object QuitEncoder : MessageEncoder<MysqlMessage.Quit, Unit> {
    override fun encode(
        value: MysqlMessage.Quit,
        buffer: ByteWriteBuffer,
        context: Unit,
    ) {
        buffer.writeByte(0x01)
    }
}
