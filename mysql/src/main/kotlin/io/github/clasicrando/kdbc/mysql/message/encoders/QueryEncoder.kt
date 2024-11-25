package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.Sink
import kotlinx.io.writeString

internal object QueryEncoder : MessageEncoder<MysqlMessage.Query, Unit> {
    override fun encode(value: MysqlMessage.Query, buffer: Sink, context: Unit) {
        buffer.writeByte(0x03)
        buffer.writeString(value.sql)
    }
}
