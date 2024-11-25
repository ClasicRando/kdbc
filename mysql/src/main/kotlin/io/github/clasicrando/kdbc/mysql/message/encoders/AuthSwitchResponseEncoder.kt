package io.github.clasicrando.kdbc.mysql.message.encoders

import io.github.clasicrando.kdbc.core.message.MessageEncoder
import io.github.clasicrando.kdbc.mysql.message.Capabilities
import io.github.clasicrando.kdbc.mysql.message.MysqlMessage
import kotlinx.io.Sink

internal object AuthSwitchResponseEncoder :
    MessageEncoder<MysqlMessage.AuthSwitchResponse, Capabilities> {
    override fun encode(
        value: MysqlMessage.AuthSwitchResponse,
        buffer: Sink,
        context: Capabilities,
    ) {
        buffer.write(value.bytes)
    }
}
