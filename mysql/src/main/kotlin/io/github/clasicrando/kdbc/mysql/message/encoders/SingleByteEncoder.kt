package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.Sink

internal object SingleByteEncoder : MessageEncoder<MysqlMessage.SingleByte, Unit> {
    override fun encode(value: MysqlMessage.SingleByte, buffer: Sink, context: Unit) {
        buffer.writeByte(value.byte)
    }
}
