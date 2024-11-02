package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.buffer.writeIntLe
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

internal object SslRequestEncoder : MessageEncoder<MysqlMessage.SslRequest, Capabilities> {
    override fun encode(
        value: MysqlMessage.SslRequest,
        buffer: ByteWriteBuffer,
        context: Capabilities,
    ) {
        buffer.writeIntLe(context.flags.toInt())
        buffer.writeIntLe(value.maxPacketSize)
        buffer.writeByte(value.collation)

        buffer.writeBytes(ByteArray(19))

        if (context[Capabilities.CLIENT_MYSQL]) {
            buffer.writeBytes(ByteArray(4))
        } else {
            buffer.writeIntLe((context.flags shr 32).toInt())
        }
    }
}
