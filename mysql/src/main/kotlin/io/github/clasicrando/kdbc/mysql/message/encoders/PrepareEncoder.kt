package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.Sink
import kotlinx.io.writeString

internal object PrepareEncoder : MessageEncoder<MysqlMessage.Prepare, Unit> {
    override fun encode(value: MysqlMessage.Prepare, buffer: Sink, context: Unit) {
        buffer.writeByte(0x16)
        buffer.writeString(value.query)
    }
}
