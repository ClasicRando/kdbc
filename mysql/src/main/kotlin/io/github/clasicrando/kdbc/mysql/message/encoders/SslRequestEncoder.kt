package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.Sink
import kotlinx.io.writeIntLe

internal object SslRequestEncoder : MessageEncoder<MysqlMessage.SslRequest, Capabilities> {
    private val EMPTY_19_BYTES = ByteArray(19)
    private val EMPTY_4_BYTES = ByteArray(4)

    override fun encode(value: MysqlMessage.SslRequest, buffer: Sink, context: Capabilities) {
        context.writeAsIntLe(buffer)
        buffer.writeIntLe(value.maxPacketSize)
        buffer.writeByte(value.characterSet)

        buffer.write(EMPTY_19_BYTES)

        if (!context[Capabilities.CLIENT_MYSQL]) {
            buffer.write(EMPTY_4_BYTES)
        } else {
            buffer.writeIntLe((context.flags shr 32).toInt())
        }
    }
}
