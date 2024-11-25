package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.Sink

internal object MultiByteEncoder : MessageEncoder<MysqlMessage.MultiByte, Unit> {
    override fun encode(value: MysqlMessage.MultiByte, buffer: Sink, context: Unit) {
        buffer.write(value.bytes)
    }
}
