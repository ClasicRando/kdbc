package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.Sink

internal object QuitEncoder : MessageEncoder<MysqlMessage.Quit, Unit> {
    override fun encode(value: MysqlMessage.Quit, buffer: Sink, context: Unit) {
        buffer.writeByte(0x01)
    }
}
