package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.Sink

internal object PingEncoder : MessageEncoder<MysqlMessage.Ping, Unit> {
    override fun encode(value: MysqlMessage.Ping, buffer: Sink, context: Unit) {
        buffer.writeByte(0x0e)
    }
}
