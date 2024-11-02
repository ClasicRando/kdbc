package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.buffer.ByteWriteBuffer
import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage

internal object AuthSwitchResponseEncoder :
    MessageEncoder<MysqlMessage.AuthSwitchResponse, Capabilities> {
    override fun encode(
        value: MysqlMessage.AuthSwitchResponse,
        buffer: ByteWriteBuffer,
        context: Capabilities,
    ) {
        buffer.writeBytes(value.bytes)
    }
}
